/*
    Author : Omar waheed

    Description: This file contains all the javascript for sidebar menu
                 To add something to sidebar you just need to make changes
                 in side_menu_json

                 Note : There is an assumption that the following exists in
                 the .html file at the appropriate position: 
                 <div id="sb" class="col-sm-2 menu-sidebar absolute title-font">
                 </div> 
*/

// position at which we change from fixed to floating menu or vice versa
var changePosition;
var sidebar_menu;
var main_div_classes = "menu-item green-hover";
var a_tag_classes = "cover-div"
var i_tag_classes = "btn-sm menu-expand-icon custom-green fa fa-plus"
var ul_tag_class = "menu-sub-item";
var main_sidebar_menu_div_reference;
var sidebar_menu;
var changePosition;
var current_selceted_menu;
var MENU_TOP_OFFSET = 27; // gap between top header and menu bar

//create and returns a guid 
function guid() {
    function s4() {
        return Math.floor((1 + Math.random()) * 0x10000)
            .toString(16)
            .substring(1);
    }
    return "guid_" + s4() + s4() + '-' + s4() + '-' + s4() + '-' +
        s4() + '-' + s4() + s4() + s4();
}

/*
       Access like sidemenu_menu_items[0].title and so on

        description:
        A JSON object that hold all the info for sidebar menu
    
        Structure:-

        num_of_tabs (int) : number of total tabs in the menu
        title (string) : the title of the main tab
        parent_href (html link) : href of the main tavpage
        recognizer: Uswed to recognize which page a sub meny belongs to and if available, expands that sybmenu when on that page or sub 
        submenu_items (array) : name of items in the submenu (order matters)
        submenu_links (array) : links of the items in the submenu
                        (order of links should be same as in submenu_items)
*/
var side_menu_json = {
    num_of_tabs: 4,

    0: {
        title: "Introduction",
        parent_href: "index.html",
        recognizer : "introduction",
        submenu_items: [],
        submenu_links: []

    },
    1: {
        title: "Getting Started",
        parent_href: "getting_started_installation.html",
        recognizer : "getting_started",
        submenu_items: ["Installation", "Tutorial"],
        submenu_links: ["getting_started_installation.html", "getting_started_tutorial.html"]
    },

    2: {
        title: "Documentation",
        parent_href: "documentation_indexing.html",
        recognizer : "documentation",
        submenu_items: ["Indexing"],
        submenu_links: ["documentation_indexing.html"]
    },

    3: {
        title: "Road Map",
        parent_href: "roadmap.html",
        recognizer : "roadmap",
        submenu_items: [],
        submenu_links: []
    }
}

/*
    Adds toggle functionality to the button with button_id
    which will open-up/close (toggle) a list or any other
    element given in  to_be_opened_list_id
*/
function add_toggle_functionality(button_id, to_be_opened_list_id) {
    var button_id_with_hash = "#" + button_id;
    var list_id_with_hash = "#" + to_be_opened_list_id

    $(button_id_with_hash).on("click", function() {
        $(list_id_with_hash).toggle();
    });
}

/*
create_sidebar_menu(sidebar_menu_items_json, append_to_element_id) {

Input:-
sidebar_menu_items_json: The json which contains the struture of 
                         all the menu tabs and their sub menu's

append_to_element_id: The element (div) to which we have to append
                      our to be built sidebar menu content

Description:-
check if submenu_items length > 0
if no
     then build and append the current frag to our sidebar
     continue
otherwise
    create main div fragment
    create anchor tag div
    create icon fragment
    create ul and then populate it with a tags containing the sub links
    Finally append it to the sidebar div
    Bind the toggle functionality to the button that open up the sub menu
*/

function create_sidebar_menu(sidebar_menu_items_json, append_to_element_id) {

    var num_of_tabs = sidebar_menu_items_json.num_of_tabs;
    var main_div_frag;
    var anchor_tag_frag;
    var icon_tag_frag;
    var icon_tag_guid_id;
    var ul_tag_frag;
    var submenu_item;
    var submenu_link;
    for (var i = 0; i < num_of_tabs; i++) {
        main_div_frag = document.createElement('div');
        main_div_frag.className = main_div_classes;
        anchor_tag_frag = document.createElement('a');
        anchor_tag_frag.className = a_tag_classes
        anchor_tag_frag.href = sidebar_menu_items_json[i].parent_href
        anchor_tag_frag.innerHTML = sidebar_menu_items_json[i].title
        if (sidebar_menu_items_json[i].submenu_items.length <= 0) {
            //append current frags and continue
            main_div_frag.appendChild(anchor_tag_frag);
            main_sidebar_menu_div_reference.append(main_div_frag);
            continue;
        }
        icon_tag_frag = document.createElement('i');
        icon_tag_frag.className = i_tag_classes;
        //generate a GUID for icon tag so that we can add a 
        // toggle function to it by id reference TO-DO
        icon_tag_frag.id = guid()
        ul_tag_frag = document.createElement('ul');
        ul_tag_frag.className = ul_tag_class;
        ul_tag_frag.id = guid()

        // main_sidebar_menu_div_reference
        //add submenu item for current tab
        for (var j = 0; j < sidebar_menu_items_json[i].submenu_items.length; j++) {
            list_element_frag = document.createElement('li');
            list_element_anchor_frag = document.createElement('a');
            submenu_item = sidebar_menu_items_json[i].submenu_items[j];
            submenu_link = sidebar_menu_items_json[i].submenu_links[j];
            list_element_anchor_frag.innerHTML = submenu_item;
            list_element_anchor_frag.href = submenu_link;
            list_element_frag.appendChild(list_element_anchor_frag)
            ul_tag_frag.appendChild(list_element_frag);
        }

        //append all the current frags to main
        main_div_frag.appendChild(anchor_tag_frag);
        main_div_frag.appendChild(icon_tag_frag);
        main_div_frag.appendChild(ul_tag_frag);
        main_sidebar_menu_div_reference.append(main_div_frag);
        add_toggle_functionality(icon_tag_frag.id, ul_tag_frag.id)
        //click and open the sub-menu if href contains a substring that matches the submenu recoginer
        if(location.href.indexOf(sidebar_menu_items_json[i].recognizer) >= 0){
            click_button(icon_tag_frag.id)
        }
    }
}

//clicks a button that 
function click_button(button_id){
    var id_with_hash = "#" + button_id;
    $( id_with_hash ).click();
}

/*
  All funcs here are executed when document is ready,
  making sure we don't reference anything that is
  undefined due to document not fully loaded
*/
$(document).ready(function() {
    main_sidebar_menu_div_reference = $("#sb");
    create_sidebar_menu(side_menu_json, main_sidebar_menu_div_reference);

    sidebar_menu = $(".menu-sidebar")[0];

    // change position is the bottom y-coordinate at which sidebar menu
    // changes from fixed to floating
    changePosition = $(".page-header")[0].getBoundingClientRect().bottom + MENU_TOP_OFFSET;

    //If page reloads always go to the top
    //of the page, otherwise sidebar menu
    //messes up
    $(window).on('beforeunload', function() {
        $(window).scrollTop(0);
    });
    //set the initial top margin of sidebar menu to bottom of our logo section
    sidebar_menu.style.top = ($(".page-header")[0].getBoundingClientRect().bottom + MENU_TOP_OFFSET) + "px";

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
                // $(".menu-sidebar")[0].style.display = "block"
                document.getElementsByClassName("menu-sidebar")[0].className = "menu-sidebar " + "fixed"
                document.getElementsByClassName("menu-sidebar")[0].style.top = "0px";
            }

            if ($(window).width() > 575 &&
                window.getComputedStyle(sidebar_menu).getPropertyValue('position') != "absolute" &&
                $(".page-header")[0].getBoundingClientRect().bottom + MENU_TOP_OFFSET >= 0) {
                document.getElementsByClassName("menu-sidebar")[0].className = "menu-sidebar " + "absolute"
                document.getElementsByClassName("menu-sidebar")[0].style.top = changePosition + "px";
            }

        }
    });
});
