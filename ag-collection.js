import jsonStableStringify from '../sc-json-stable-stringify/sc-json-stable-stringify.js';
import AsyncStreamEmitter from '../async-stream-emitter/async-stream-emitter.min.js';
import AGModel from '../ag-model/ag-model.js';

function AGCollection(options) {
  AsyncStreamEmitter.call(this);

  this.isActive = true;
  this.socket = options.socket;
  this.type = options.type;
  this.fields = options.fields || [];
  this.fieldTransformations = options.fieldTransformations || {};
  this.isLoaded = false;
  this.defaultFieldValues = options.defaultFieldValues;
  this.view = options.view;
  this.viewParams = options.viewParams;
  if (options.viewPrimaryFields == null) {
    this.viewPrimaryFields = Object.keys(options.viewParams || {});
  } else {
    this.viewPrimaryFields = options.viewPrimaryFields;
  }
  this.meta = {
    type: this.type,
    view: this.view,
    viewParams: this.viewParams,
    viewPrimaryFields: this.viewPrimaryFields,
    pageOffset: options.pageOffset || 0,
    pageSize: options.pageSize == null ? 10 : options.pageSize,
    isLastPage: null,
    count: null
  };
  this.getCount = options.getCount;
  this.realtimeCollection = options.realtimeCollection == null ? true : options.realtimeCollection;
  this.writeOnly = options.writeOnly;
  this.changeReloadDelay = options.changeReloadDelay == null ? 300 : options.changeReloadDelay;
  this.passiveMode = options.passiveMode || false;

  this.agModels = {};
  this.value = [];

  this._channelOutputConsumerIds = [];
  this._channelListenerConsumerIds = [];
  this._socketListenerConsumerIds = [];

  this._triggerCollectionError = (error) => {
    this.emit('error', {error: this._formatError(error)});
  };

  this._handleAGModelError = (error) => {
    this._triggerCollectionError(error);
  };

  this._updateModelIsLoaded = () => {
    if (this.isLoaded) {
      this.isLoaded = Object.values(this.agModels).every(model => model.isLoaded);
    } else {
      this.isLoaded = Object.values(this.agModels).every(model => model.isLoaded);
      if (this.isLoaded) {
        this.emit('load', {});
      }
    }
  };

  this._handleAGModelChange = (event) => {
    this.value.forEach((modelValue, index) => {
      if (modelValue.id === event.resourceId) {
        this.value.splice(index, 1, modelValue);
      }
    });

    this._updateModelIsLoaded();

    this.emit('modelChange', event);
    this.emit('change', event);
  };

  if (this.writeOnly) {
    return;
  }

  if (!this.realtimeCollection) {
    // This is to account for socket reconnects - After recovering from a lost connection,
    // we will re-fetch the whole value to make sure that we haven't missed any updates made to it.
    // This is not necessary for a realtime collection because it relies on subscribe events instead.
    (async () => {
      let consumer = this.socket.listener('connect').createConsumer();
      this._socketListenerConsumerIds.push(consumer.id);
      while (true) {
        let packet = await consumer.next();
        if (packet.done) {
          if (!this.isActive) {
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
  let subscribeOptions = {
    data: {
      viewParams: viewParamsObject
    }
  };
  let viewPrimaryParams = {};

  this.viewPrimaryFields.forEach(function (field) {
    viewPrimaryParams[field] = viewParamsObject[field] === undefined ? null : viewParamsObject[field];
  });
  if (!options.typedViewChannelParams) {
    for (let [key, value] of Object.entries(viewPrimaryParams)) {
      viewPrimaryParams[key] = String(value);
    }
  }

  let viewPrimaryParamsString = jsonStableStringify(viewPrimaryParams);
  let viewChannelName = `${channelPrefix}${this.view}(${viewPrimaryParamsString}):${this.type}`;

  this.channel = this.socket.subscribe(viewChannelName, subscribeOptions);

  this._symbol = Symbol();

  if (!this.socket.channelWatchers) {
    this.socket.channelWatchers = {};
  }
  if (!this.socket.channelWatchers[this.channel.name]) {
    this.socket.channelWatchers[this.channel.name] = {};
  }
  this.socket.channelWatchers[this.channel.name][this._symbol] = true;

  this._reloadTimeoutId = null;

  (async () => {
    let consumer = this.channel.createConsumer();
    this._channelOutputConsumerIds.push(consumer.id);
    while (true) {
      let packet = await consumer.next();
      if (packet.done) {
        if (!this.isActive) {
          if (this._reloadTimeoutId != null) {
            clearTimeout(this._reloadTimeoutId);
          }
          break;
        }
      } else {
        (async () => {
          if (
            packet.value &&
            packet.value.type === 'delete' &&
            packet.value.value &&
            packet.value.value.id &&
            this.agModels[packet.value.value.id]
          ) {
            // Do not allow a model to update itself while the collection
            // is in the middle of refreshing itself to delete it.
            this.agModels[packet.value.value.id].destroy();
            delete this.agModels[packet.value.value.id];
          }
          this.isLoaded = false;
          if (this._reloadTimeoutId != null) {
            return;
          }
          let {timeoutId, promise} = this._wait(this.changeReloadDelay);
          this._reloadTimeoutId = timeoutId;
          await promise;
          this._reloadTimeoutId = null;
          this.reloadCurrentPage();
        })();
      }
    }
  })();

  // The purpose of useFastInitLoad is to reduce latency of the initial load
  // when the collection is first created.
  let useFastInitLoad;

  if (this.socket.state == 'open') {
    this.loadData();
    useFastInitLoad = true;
  } else {
    useFastInitLoad = false;
  }

  (async () => {
    let consumer = this.channel.listener('subscribe').createConsumer();
    this._channelListenerConsumerIds.push(consumer.id);
    while (true) {
      let packet = await consumer.next();
      if (packet.done) {
        if (!this.isActive) {
          break;
        }
      } else {
        // Fetch data when subscribe is successful.
        // If useFastInitLoad was used, then do not load again the first time.
        if (useFastInitLoad) {
          useFastInitLoad = false;
        } else {
          this.loadData();
        }
      }
    }
  })();

  (async () => {
    let consumer = this.channel.listener('subscribeFail').createConsumer();
    this._channelListenerConsumerIds.push(consumer.id);
    while (true) {
      let packet = await consumer.next();
      if (packet.done) {
        if (!this.isActive) {
          break;
        }
      } else {
        useFastInitLoad = false;
        this._triggerCollectionError(packet.value.error);
      }
    }
  })();

  (async () => {
    let consumer = this.socket.listener('close').createConsumer();
    this._socketListenerConsumerIds.push(consumer.id);
    while (true) {
      let packet = await consumer.next();
      if (packet.done) {
        if (!this.isActive) {
          break;
        }
      } else {
        useFastInitLoad = false;
      }
    }
  })();

  (async () => {
    let consumer = this.socket.listener('authenticate').createConsumer();
    this._socketListenerConsumerIds.push(consumer.id);
    while (true) {
      let packet = await consumer.next();
      if (packet.done) {
        if (!this.isActive) {
          break;
        }
      } else {
        this.channel.subscribe({
          data: {
            viewParams: this.viewParams || {}
          }
        });
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

AGCollection.prototype._wait = function (timeout) {
  let timeoutId;
  let promise = new Promise((resolve) => {
    timeoutId = setTimeout(resolve, timeout);
  });
  return {timeoutId, promise};
};

// Load values for the collection.
AGCollection.prototype.loadData = async function () {
  if (this.writeOnly) {
    this._triggerCollectionError('Cannot load values for an AGCollection declared as write-only');
    return;
  }
  this.isLoaded = false;

  let query = {
    action: 'read',
    type: this.type
  };
  query.offset = this.meta.pageOffset || 0;
  if (this.view != null) {
    query.view = this.view;
  }
  if (this.viewParams != null) {
    query.viewParams = this.viewParams;
  }
  if (this.meta.pageSize != null) {
    query.pageSize = this.meta.pageSize;
  }
  if (this.getCount) {
    query.getCount = true;
  }

  let result;
  try {
    result = await this.socket.invoke('crud', query);
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

  let createdModels = [];

  for (let i = 0; i < resultDataLen; i++) {
    let tempId = result.data[i];
    newIdsLookup[tempId] = true;
    if (existingItemsMap[tempId] == null) {
      let model = new AGModel({
        socket: this.socket,
        type: this.type,
        id: tempId,
        fields: this.fields,
        fieldTransformations: this.fieldTransformations,
        defaultFieldValues: this.defaultFieldValues,
        passiveMode: this.passiveMode
      });
      createdModels.push(model);
      this.agModels[tempId] = model;
      this.value.push(model.value);

      (async () => {
        for await (let {error} of model.listener('error')) {
          this._handleAGModelError(error);
        }
      })();

      (async () => {
        for await (let event of model.listener('load')) {
          this._updateModelIsLoaded();
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

  let deletedModels = [];

  Object.keys(this.agModels).forEach((resourceId) => {
    if (!newIdsLookup[resourceId]) {
      let model = this.agModels[resourceId];
      model.destroy();
      delete this.agModels[resourceId];
      deletedModels.push(model);
      this.emit('modelDestroy', model);
    }
  });

  if (result.count != null) {
    this.meta.count = result.count;
  }

  let oldLastPageState = this.meta.isLastPage;
  this.meta.isLastPage = result.isLastPage;

  let oldStateString = oldValue.map(resource => resource.id).join(',');
  let currentStateString = this.value.map(resource => resource.id).join(',');

  this._updateModelIsLoaded();

  if (oldStateString === currentStateString && oldLastPageState === this.meta.isLastPage) {
    return;
  };

  let event = {
    resourceType: this.type,
    oldValue,
    newValue: this.value,
    createdModels,
    deletedModels,
    isRemote: true
  };

  this.emit('collectionChange', event);
  this.emit('change', event);
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
    // This is needed to avoid flickering due to network latency.
    this.meta.isLastPage = false;
    this.meta.pageOffset = prevOffset;
    this.reloadCurrentPage();
  }
};

AGCollection.prototype.fetchPage = function (offset) {
  if (offset < 0) {
    offset = 0;
  }
  this.meta.pageOffset = offset;
  this.reloadCurrentPage();
};

AGCollection.prototype.create = async function (newValue) {
  let query = {
    action: 'create',
    type: this.type,
    value: newValue
  };
  return this.socket.invoke('crud', query);
};

AGCollection.prototype.update = function (id, newValue) {
  let activeModel = this.agModels[id];
  if (activeModel) {
    return Promise.all(
      Object.entries(newValue || {}).map(([ key, value ]) => {
        return activeModel.update(key, value);
      })
    );
  }
  let query = {
    action: 'update',
    type: this.type,
    id,
    value: newValue
  };
  return this.socket.invoke('crud', query);
};

AGCollection.prototype.delete = function (id, field) {
  let query;
  if (field == null) {
    query = {
      action: 'delete',
      type: this.type,
      id
    };
  } else {
    let activeModel = this.agModels[id];
    if (activeModel) {
      return activeModel.delete(field);
    }
    query = {
      action: 'delete',
      type: this.type,
      id,
      field
    };
  }
  return this.socket.invoke('crud', query);
};

AGCollection.prototype.destroy = async function () {
  this.killAllListeners();
  if (!this.isActive) {
    return;
  }
  this.isActive = false;

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
  await Promise.all(
    Object.values(this.agModels).map(async (model) => {
      await model.destroy();
      this.emit('modelDestroy', model);
    })
  );
};

export default AGCollection;
