$('.tree .icon').click( function() {
  $(this).parent().toggleClass('expanded').
  closest('li').find('ul:first').
  toggleClass('show-effect');
});
