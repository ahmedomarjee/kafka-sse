import Ember from 'ember';

export default Ember.Controller.extend({
  actions: {
    filterByContent(param) {
      if (param !== '') {
        var events = this.get('store').peekAll('event');
        var promise = new Promise(function(resolve, reject) {
          var data = events.filter( function(item){ return item.get('msg').indexOf(param) !== -1 });
          resolve(data);
          // on failure
          reject([]);
        });
        return promise;
      } else {
        var store = this.get('store');
        var promise = new Promise(function(resolve, reject) {
          // on success
          resolve(store.peekAll('event'));

          // on failure
          reject([]);
        });
        return promise;
      }
    }
  }
});
