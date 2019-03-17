import jsonStableStringify from '../sc-json-stable-stringify/sc-json-stable-stringify.js';
import AsyncStreamEmitter from '../async-stream-emitter/async-stream-emitter.js';
import AGModel from '../ag-model/ag-model.js';

function AGCollection(options) {
  AsyncStreamEmitter.call(this);

  this.active = true;
  this.socket = options.socket;
  this.type = options.type;
  this.fields = options.fields;
  this.view = options.view;
  this.viewParams = options.viewParams;
  if (options.viewPrimaryKeys == null) {
    this.viewPrimaryKeys = Object.keys(options.viewParams || {});
  } else {
    this.viewPrimaryKeys = options.viewPrimaryKeys;
  }
  this.meta = {
    pageOffset: options.pageOffset || 0,
    pageSize: options.pageSize || 10,
    isLastPage: null,
    count: null
  };
  this.getCount = options.getCount;
  this.realtimeCollection = options.realtimeCollection == null ? true : options.realtimeCollection;
  this.writeOnly = options.writeOnly;

  this.agModel = {};
  this.value = [];

  this._channelOutputConsumerIds = [];
  this._channelListenerConsumerIds = [];
  this._socketListenerConsumerIds = [];

  this._triggerCollectionError = (error) => {
    this.emit('error', this._formatError(error));
  };

  this._handleAGModelError = (error) => {
    this._triggerCollectionError(error);
  };

  this._handleAGModelChange = (event) => {
    this.value.forEach((modelValue, index) => {
      if (modelValue.id === event.resourceId) {
        this.value.splice(index, 1, modelValue);
      }
    });
    this.emit('modelChange', event);
  };

  if (this.writeOnly) {
    return;
  }

  if (!this.realtimeCollection) {
    // This is to account for socket reconnects - After recovering from a lost connection,
    // we will re-fetch the whole value to make sure that we haven't missed any updates made to it.
    (async () => {
      let consumer = this.socket.listener('connect').createConsumer();
      this._socketListenerConsumerIds.push(consumer.id);
      while (true) {
        let packet = await consumer.next();
        if (packet.done) {
          if (!this.active) {
            break;
          }
        } else {
          this.loadData();
        }
      }
    })();

    if (this.socket.state == 'open') {
      this.loadData();
    }
    return;
  }

  let channelPrefix = 'crud>';
  let viewParamsObject = this.viewParams || {};
  let viewPrimaryParams = {};

  this.viewPrimaryKeys.forEach(function (field) {
    viewPrimaryParams[field] = viewParamsObject[field] === undefined ? null : viewParamsObject[field];
  });
  let viewPrimaryParamsString = jsonStableStringify(viewPrimaryParams);
  let viewChannelName = `${channelPrefix}${this.view}(${viewPrimaryParamsString}):${this.type}`;

  let subscribeOptions = {
    data: {
      viewParams: viewParamsObject
    }
  };

  this.channel = this.socket.subscribe(viewChannelName, subscribeOptions);

  this._symbol = Symbol();

  if (!this.socket.channelWatchers) {
    this.socket.channelWatchers = {};
  }
  if (!this.socket.channelWatchers[this.channel.name]) {
    this.socket.channelWatchers[this.channel.name] = {};
  }
  this.socket.channelWatchers[this.channel.name][this._symbol] = true;

  (async () => {
    let consumer = this.channel.createConsumer();
    this._channelOutputConsumerIds.push(consumer.id);
    while (true) {
      let packet = await consumer.next();
      if (packet.done) {
        if (!this.active) {
          break;
        }
      } else {
        this.reloadCurrentPage();
      }
    }
  })();

  (async () => {
    let consumer = this.channel.listener('subscribe').createConsumer();
    this._channelListenerConsumerIds.push(consumer.id);
    while (true) {
      let packet = await consumer.next();
      if (packet.done) {
        if (!this.active) {
          break;
        }
      } else {
        // Fetch data when subscribe is successful.
        this.loadData();
      }
    }
  })();

  if (this.channel.state === 'subscribed') {
    this.loadData();
  }

  (async () => {
    let consumer = this.channel.listener('subscribeFail').createConsumer();
    this._channelListenerConsumerIds.push(consumer.id);
    while (true) {
      let packet = await consumer.next();
      if (packet.done) {
        if (!this.active) {
          break;
        }
      } else {
        this._triggerCollectionError(packet.value.error);
      }
    }
  })();

  (async () => {
    let consumer = this.socket.listener('authenticate').createConsumer();
    this._socketListenerConsumerIds.push(consumer.id);
    while (true) {
      let packet = await consumer.next();
      if (packet.done) {
        if (!this.active) {
          break;
        }
      } else {
        this.channel.subscribe();
      }
    }
  })();
}

AGCollection.prototype = Object.create(AsyncStreamEmitter.prototype);

AGCollection.AsyncStreamEmitter = AsyncStreamEmitter;

AGCollection.prototype._formatError = function (error) {
  if (error) {
    if (error.message) {
      return new Error(error.message);
    }
    return new Error(error);
  }
  return error;
};

// Load values for the collection.
AGCollection.prototype.loadData = async function () {
  if (this.writeOnly) {
    this._triggerCollectionError('Cannot load values for an AGCollection declared as write-only');
    return;
  }

  let query = {
    type: this.type
  };
  query.offset = this.meta.pageOffset || 0;
  if (this.view != null) {
    query.view = this.view;
  }
  if (this.viewParams != null) {
    query.viewParams = this.viewParams;
  }
  if (this.meta.pageSize) {
    query.pageSize = this.meta.pageSize;
  }
  if (this.getCount) {
    query.getCount = true;
  }

  let result;
  try {
    result = await this.socket.invoke('read', query);
  } catch (error) {
    this._triggerCollectionError(error);

    return;
  }

  let existingItemsMap = {};
  let newIdsLookup = {};
  let currentItems = this.value;
  let len = currentItems.length;

  for (let h = 0; h < len; h++) {
    existingItemsMap[currentItems[h].id] = currentItems[h];
  }

  let oldValue = this.value.splice(0);
  let resultDataLen = result.data.length;

  for (let i = 0; i < resultDataLen; i++) {
    let tempId = result.data[i];
    newIdsLookup[tempId] = true;
    if (existingItemsMap[tempId] == null) {
      let model = new AGModel({
        socket: this.socket,
        type: this.type,
        id: tempId,
        fields: this.fields
      });
      this.agModel[tempId] = model;
      this.value.push(model.value);

      (async () => {
        for await (let {error} of model.listener('error')) {
          this._handleAGModelError(error);
        }
      })();

      (async () => {
        for await (let event of model.listener('change')) {
          this._handleAGModelChange(event);
        }
      })();

    } else {
      this.value.push(existingItemsMap[tempId]);
    }
  }

  Object.keys(this.agModel).forEach((resourceId) => {
    if (!newIdsLookup[resourceId]) {
      this.agModel[resourceId].destroy();
      delete this.agModel[resourceId];
    }
  });

  if (result.count != null) {
    this.meta.count = result.count;
  }

  this.emit('change', {
    resourceType: this.type,
    oldValue: oldValue,
    newValue: this.value
  });

  this.meta.isLastPage = result.isLastPage;
};

AGCollection.prototype.reloadCurrentPage = function () {
  if (!this.writeOnly) {
    this.loadData();
  }
};

AGCollection.prototype.fetchNextPage = function () {
  if (!this.meta.isLastPage) {
    this.meta.pageOffset += this.meta.pageSize;
    this.reloadCurrentPage();
  }
};

AGCollection.prototype.fetchPreviousPage = function () {
  if (this.meta.pageOffset > 0) {
    let prevOffset = this.meta.pageOffset - this.meta.pageSize;
    if (prevOffset < 0) {
      prevOffset = 0;
    }
    this.meta.pageOffset = prevOffset;
    this.reloadCurrentPage();
  }
};

AGCollection.prototype.create = async function (newValue) {
  let query = {
    type: this.type,
    value: newValue
  };
  return this.socket.invoke('create', query);
};

AGCollection.prototype.delete = function (id) {
  let query = {
    type: this.type,
    id: id
  };
  return this.socket.invoke('delete', query);
};

AGCollection.prototype.destroy = function () {
  if (!this.active) {
    return;
  }
  this.active = false;

  this._socketListenerConsumerIds.forEach((consumerId) => {
    this.socket.killListenerConsumer(consumerId);
  });
  if (this.channel) {
    this._channelOutputConsumerIds.forEach((consumerId) => {
      this.channel.killOutputConsumer(consumerId);
    });
    this._channelListenerConsumerIds.forEach((consumerId) => {
      this.channel.killListenerConsumer(consumerId);
    });

    let watchers = this.socket.channelWatchers[this.channel.name];
    if (watchers) {
      delete watchers[this._symbol];
    }
    if (!Object.getOwnPropertySymbols(watchers || {}).length) {
      delete this.socket.channelWatchers[this.channel.name];
      this.channel.unsubscribe();
    }
  }
  Object.values(this.agModel).forEach((agModel) => {
    agModel.killListener('error');
    agModel.killListener('change');
    agModel.destroy();
  });
};

export default AGCollection;
