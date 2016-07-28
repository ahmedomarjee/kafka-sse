import Ember from 'ember';

export default Ember.Route.extend({
  model: function(){
    console.log(this);
    return this.store.peekAll('event');
  },
  activate: function(){
    var the_store = this.store;
    
    console.log(this);
    var source = new EventSource("https://kafka-sse.fc-uat.co.uk");
    var limit = 1000;
    var count = 0;
    source.onmessage = function(e) {
      //the_store.push({data:[{type: 'event', attributes: {offset:1, msg:'hi)'}, id: 1}]});
      if(count < limit) {
        the_store.createRecord('event', {offset:1, msg: e.data});
        count ++;
      } else {
        source.close();
      }
    }
  }
});
