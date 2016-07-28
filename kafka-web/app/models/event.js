import DS from 'ember-data';

export default DS.Model.extend({
  offset: DS.attr(),
  msg: DS.attr(),
});
