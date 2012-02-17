//Additional commands for layout

$(function(){
    $('.brand').attr('href', '/');
    $('h2, h3').addClass('btn').css({'font-size' : '130%', 'margin-bottom' : '12px'});
    $('h4').addClass('btn').css({'font-size' : '90%', 'margin-bottom' : '10px'});
    $('body>div.container').css({'padding-left' : '30px'});
    $('.topbar-inner').find('.container').css({'width': 'auto', 'margin-right': '30px', 'padding-left': '188px'});
    $('th').addClass('alert-message');

//Fix for overextended menu
    var local_menu = $('span.localtoc > ul');
    if(local_menu.length > 0){
        $('.dropdown').click(function(){
            if($('.localtoc').find('li').length > 15){
            $('.localtoc').find('ul').filter(':first').css({'max-height': '680px', 'overflow-y': 'scroll'});
            }
        });
    }
});
