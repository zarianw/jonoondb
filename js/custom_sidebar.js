/*
    Author : Omar waheed

    Description: This file contains all the javascript for sidebar menu
*/

// position at which we change from fixed to floating menu or vice versa
var changePosition;
var sidebar_menu;

/*
Setup variables to add submenu items.
name array contains the name that will show on the menu.
link array show links that the name points to.

If you add or delete a menu element then

(1) add/remove its name and link array
(2) object array
(3) Store plus button id in a plus button var 
(4) Add entry in show_or_hide_menu_expand_btn()
(5) Add/remove the toggle() functionality  
(6) Add entry in build_sub_menu_tree()

Note : there are two menu's (fixed and absolute) 
        so do this for both of the menu
*/

//introduction
var intro_submenu_name = ['test1', 'test2', 'Another link'];
var intro_submenu_links = ['index.html', 'tutorial.html', 'https://www.google.com'];
//tutorial
var tutorial_submenu_name = ['test3', 'test4', 'Another link'];
var tutorial_submenu_links = ['index.html', 'tutorial.html', 'https://www.google.com'];
//About (no sub menu items)
var getting_started_submenu_name = ['Installation', 'Tutorial'];
var getting_started_submenu_links = ['getting_started_installation.html', 'getting_started_tutorial.html'];
//Documentation
var documentation_submenu_name = ['Indexing'];
var documentation_submenu_links = ['documentation_indexing.html'];

//plus buttons that expands the sub-menu section
var intro_plus_btn;
var intro_plus_btn_2;
var getting_started_plus_btn;
var getting_started_plus_btn_2;
var tutorial_plus_btn;
var tutorial_plus_btn_2;
var documentation_plus_btn;
var documentation_plus_btn_2;


/*  Creates an object if submenu item
    contains two properties
    Args:-
        name : name of the sub menu item
        link : link of the sub menu item
*/
var submenu_item = function(name, link) {
    this.name = name
    this.link = link
}

//creates an array of submenu_items
function create_menu_objs(names, links) {
    if (!names || !links) {
        return undefined;
    }
    var result_array = []
    for (var i = 0; i < names.length; i++) {
        console.log("name is : " + names[i])
        result_array[i] = new submenu_item(names[i], links[i])
    }
    return result_array;
}

var intro_sub_menu_objs_array = create_menu_objs(intro_submenu_name, intro_submenu_links)
var tutorial_sub_menu_objs_array = create_menu_objs(tutorial_submenu_name, tutorial_submenu_links)
var getting_started_sub_menu_objs_array = create_menu_objs(getting_started_submenu_name, getting_started_submenu_links)
var documentation_sub_menu_objs_array = create_menu_objs(documentation_submenu_name, documentation_submenu_links)

/*
  function addSubMenuItems


  Description: 
  This function takes in the menu element (e.g. introduction section)
  and an array of it's sub menu objects created using create_menu_objs() func.
  It then populates the unordered list <ul> in the html with the specified
  sub menu elements

  e.g.
  obj1 = {
    name: "test1",
    link: "www.google.com"
  }

  obj2 = {
    name: "test2",
    link: "www.jonoondb.org"
  }

  introduction_div = some div that points to the menu's "introduction" element
  then calling:-

  addSubMenuItems(introduction_div, [obj1,obj2])

  will do the following:-

  Introduction
    |_obj1
    |_obj2

    clicking test1 will open www.google.com
*/
function addSubMenuItems(element, sub_menu_objs) {

    for (var i = 0; i < sub_menu_objs.length; i++) {
        element.append('<li><a href="' + sub_menu_objs[i].link + '">' + sub_menu_objs[i].name + '</a></li>');
    }
}

// check for each menu and if any of them
// has an empty submenu item array, then
// hide the plus button from that element
// else do nothing as by default plus is
// visible
function show_or_hide_menu_expand_btn() {
    if (!intro_sub_menu_objs_array || intro_sub_menu_objs_array.length <= 0) {
        intro_plus_btn.css("visibility", "hidden");
        intro_plus_btn_2.css("visibility", "hidden");

    }
    if (!tutorial_sub_menu_objs_array || tutorial_sub_menu_objs_array.length <= 0) {
        tutorial_plus_btn.css("visibility", "hidden");
        tutorial_plus_btn_2.css("visibility", "hidden");
    }
    if (!getting_started_sub_menu_objs_array || getting_started_sub_menu_objs_array.length <= 0) {
        getting_started_plus_btn.css("visibility", "hidden");
        getting_started_plus_btn_2.css("visibility", "hidden");
    }
    if (!documentation_sub_menu_objs_array || documentation_sub_menu_objs_array.length <= 0) {
        documentation_plus_btn.css("visibility", "hidden");
        documentation_plus_btn_2.css("visibility", "hidden");
    }
}

/*
function build_sub_menu_tree
  description: builds the sub menu tree in the 
  sidemenu bar
*/
function build_sub_menu_tree() {

    //if these variables are needed at more than one place
    // make these global, instead of calling jquery func again
    // it's much faster that way
    var intro_ul_element_abs = $('#intro-submenu-absolute');
    var intro_ul_element_fixed = $('#intro-submenu-fixed');
    var getting_started_ul_element_abs = $('#getting_started_submenu_absolute');
    var getting_started_ul_element_fixed = $('#getting_started_submenu_fixed');
    var tutorial_ul_element_fixed = $('#getting_started_submenu_fixed');
    var tutorial_ul_element_abs = $('#tutorial-submenu-absolute');
    var tutorial_ul_element_fixed = $('#tutorial-submenu-fixed');
    var documentation_ul_element_abs = $('#documentation_submenu_absolute');
    var documentation_ul_element_fixed = $('#documentation_submenu_fixed');


    show_or_hide_menu_expand_btn()
    addSubMenuItems(intro_ul_element_abs, intro_sub_menu_objs_array);
    addSubMenuItems(intro_ul_element_fixed, intro_sub_menu_objs_array);
    addSubMenuItems(getting_started_ul_element_abs, getting_started_sub_menu_objs_array);
    addSubMenuItems(getting_started_ul_element_fixed, getting_started_sub_menu_objs_array);
    addSubMenuItems(tutorial_ul_element_abs, tutorial_sub_menu_objs_array);
    addSubMenuItems(tutorial_ul_element_fixed, tutorial_sub_menu_objs_array);
    addSubMenuItems(documentation_ul_element_abs, documentation_sub_menu_objs_array);
    addSubMenuItems(documentation_ul_element_fixed, documentation_sub_menu_objs_array);

}

//All funcs here are executed when document is ready,
//making sure we don't reference anything that is
//undefined due to document not fully loaded
$(document).ready(function() {
    //all the plus buttons on sidebar menu
    //add the buttons for new menu element's here and at other
    //places in the code so that the expandable menu works fine
    intro_plus_btn = $("#intro_btn");
    intro_plus_btn_2 = $("#intro_btn_2");
    getting_started_plus_btn = $('#getting_started_btn_1');
    getting_started_plus_btn_2 = $('#getting_started_btn_2')
    tutorial_plus_btn = $("#tutorial_btn");
    tutorial_plus_btn_2 = $("#tutorial_btn_2");
    documentation_plus_btn = $('#documentation_btn_1');
    documentation_plus_btn_2 = $('#documentation_btn_2');

    //build the sidebar menu's sub menu item
    build_sub_menu_tree()

    //The button binds below expand the sidebar menu's
    //sub-menu section (the one that we open by pressing the plus sign)
    intro_plus_btn.on("click", function() {
        $('#intro-submenu-absolute').toggle();
    });

    intro_plus_btn_2.on("click", function() {
        $('#intro-submenu-fixed').toggle();
    });

    getting_started_plus_btn.on("click", function() {
        $('#getting_started_submenu_absolute').toggle();
    });

    getting_started_plus_btn_2.on("click", function() {
        $('#getting_started_submenu_fixed').toggle();
    });

    documentation_plus_btn.on("click", function() {
        $('#documentation_submenu_absolute').toggle();
    });

    documentation_plus_btn_2.on("click", function() {
        $('#documentation_submenu_fixed').toggle();
    });

    tutorial_plus_btn.on("click", function() {
        $('#tutorial-submenu-absolute').toggle();
    });

    tutorial_plus_btn_2.on("click", function() {
        $('#tutorial-submenu-fixed').toggle();
    });

    sidebar_menu = $(".menu-sidebar")[0];

    // change position is the bottom y-coordinate at which sidebar menu
    // changes from fixed to floating
    changePosition = $(".page-header")[0].getBoundingClientRect().bottom;

    //If page reloads always go to the top
    //of the page, otherwise sidebar menu
    //messes up
    $(window).on('beforeunload', function() {
        $(window).scrollTop(0);
    });
    //set the initial top margin of sidebar menu to bottom of our logo section
    sidebar_menu.style.top = $(".page-header")[0].getBoundingClientRect().bottom + "px";

    //scroll function is executed at every scroll 
    $(window).scroll(function() {
        var myDiv = sidebar_menu

        if (($(window).width() < 576 || $(window).width() > 678) && ($(window).width() > 765 && /chrom(e|ium)/.test(navigator.userAgent.toLowerCase()))) {
            //only do this logic in chrome.
            //fall back to reponsive design for other browsers
            //if top is > sidebar menu's top then move it

            //set sidebar menu to fixed if we reach the specified upper part of page
            //so that we dont collide into logo section of page
            if (window.getComputedStyle(sidebar_menu).getPropertyValue('position') != "fixed" &&
                $(window).width() > 575 &&
                window.pageYOffset >= changePosition) {
                $(".menu-sidebar")[1].style.display = "block"
                sidebar_menu.style.display = "none"
                sidebar_menu = $(".menu-sidebar")[1];
            }

            // Activate the floating menu if the logo section 
            // is past the visible page area
            if ($(window).width() > 575 &&
                window.getComputedStyle(sidebar_menu).getPropertyValue('position') != "absolute" &&
                $(".page-header")[0].getBoundingClientRect().bottom >= 0) {
                $(".menu-sidebar")[0].style.display = "block";
                sidebar_menu.style.display = "none"
                sidebar_menu = $(".menu-sidebar")[0];
            }

        }
    });


});
