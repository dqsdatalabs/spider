# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'sheadproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en' 
    external_source='Sheadproperty_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        headers={
                "Accept": "*/*",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Cookie": "_ga=GA1.3.1789102035.1641897365; _gid=GA1.3.1520681128.1641897365; _gat=1",
                "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Mobile Safari/537.36",
                "X-Requested-With": "XMLHttpRequest",
            }
        formdata = {
                "atts[list]": "lease",
                "atts[multilist]": "",
                "atts[layout]": "",
                "atts[template]": "Shortcode.SearchResults.SearchResults",
                "atts[selector_listings]": "ap-listing-search-results",
                "atts[ajax_template]": "Ajax.SearchResults",
                "atts[load_more]": "true",
                "atts[hide_search_form]": "0",
                "atts[row_col_class]": "row row-cols-xl-3 row-cols-lg-2 row-cols-1",
                "atts[per_page]": "6",
                "atts[sur_suburbs]": "0",
                "atts[center_latlng]": "",
                "atts[sur_suburbs_radius]": "10",
                "atts[max_page]": "false",
                "atts[map_zoom]": "11",
                "atts[map]": "map_canvas",
                "atts[map_load_all_marker]": "0",
                "atts[map_callback]": "ap_realty.searchResultsMapCallback",
                "atts[map_attribute_cluster]": "0",
                "atts[map_is_visible]": "1",
                "atts[property_type_column]": "1,2,3",
                "atts[content_first]": "0",
                "atts[auth]": "0",
                "atts[auth_type]": "content",
                "atts[auth_message]": "Sorry, You Are Not Allowed to Access This Page",
                "atts[map_attribute_loadCallback]": "ap_realty.searchResultsMapCallback",
                "atts[type]": "ResidentialLease",
                "atts[sort]": "latest",
                "per_page": "6",
                "load_more": "true",
                "selectorMap": "#map_canvas",
                "selector": "#ap-listing-search-results",
                "current_page": "1",
                "sort": "latest",
                "action": "property_search_results",
            }

        url = "https://www.shead.com.au/wp-admin/admin-ajax.php"
        yield FormRequest(url,headers=headers,formdata = formdata,callback = self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        border=15
        page = response.meta.get("page", 2)
        seen = False
        data = str(response.body).split('{"html":')[-1].split(',"params":{"ajax')[0]
        for item in data.split('container p-0 bg-black-5'):
            item=item.split('title')[0].replace("n <a","").replace(" href=","").replace("\\","").replace('"',"").replace(">","")
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page == 2 or seen:
            if page<border:
                headers={
                        "Accept": "*/*",
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "Cookie": "_ga=GA1.3.1789102035.1641897365; _gid=GA1.3.1520681128.1641897365; _gat=1",
                        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Mobile Safari/537.36",
                        "X-Requested-With": "XMLHttpRequest",
                        }
                formdata = {
                        "atts[list]": "lease",
                        "atts[multilist]": "",
                        "atts[layout]": "",
                        "atts[template]": "Shortcode.SearchResults.SearchResults",
                        "atts[selector_listings]": "ap-listing-search-results",
                        "atts[ajax_template]": "Ajax.SearchResults",
                        "atts[load_more]": "true",
                        "atts[hide_search_form]": "0",
                        "atts[row_col_class]": "row row-cols-xl-3 row-cols-lg-2 row-cols-1",
                        "atts[per_page]": "6",
                        "atts[sur_suburbs]": "0",
                        "atts[center_latlng]": "",
                        "atts[sur_suburbs_radius]": "10",
                        "atts[max_page]": "false",
                        "atts[map_zoom]": "11",
                        "atts[map]": "map_canvas",
                        "atts[map_load_all_marker]": "0",
                        "atts[map_callback]": "ap_realty.searchResultsMapCallback",
                        "atts[map_attribute_cluster]": "0",
                        "atts[map_is_visible]": "1",
                        "atts[property_type_column]": "1,2,3",
                        "atts[content_first]": "0",
                        "atts[auth]": "0",
                        "atts[auth_type]": "content",
                        "atts[auth_message]": "Sorry, You Are Not Allowed to Access This Page",
                        "atts[map_attribute_loadCallback]": "ap_realty.searchResultsMapCallback",
                        "atts[type]": "ResidentialLease",
                        "atts[sort]": "latest",
                        "per_page": "6",
                        "load_more": "true",
                        "selectorMap": "#map_canvas",
                        "selector": "#ap-listing-search-results",
                        "current_page": str(page),
                        "sort": "latest",
                        "action": "property_search_results",
                    }

                url = "https://www.shead.com.au/wp-admin/admin-ajax.php"
                yield FormRequest(url,headers=headers,formdata = formdata,callback = self.parse,meta={'page':page+1})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//label[.='Location']/following-sibling::div/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        external_id=response.xpath("//label[.='Property ID']/following-sibling::div/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        rent=response.xpath("//label[.='Price']/following-sibling::div/text()").get()
        if rent and not "DEPOSIT TAKEN" in rent:
            item_loader.add_value("rent",int(rent.split("$")[-1].split("pw")[0].split("per")[0].strip())*4)
        item_loader.add_value("currency","USD")
        room_count=response.xpath("//label[.='Bedrooms']/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//label[.='Bathrooms']/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        property_type=response.xpath("//label[.='Type']/following-sibling::div/text()").get()
        if property_type and "apartment" in property_type.lower():
            item_loader.add_value("property_type","apartment")
        if property_type and "unit" in property_type.lower():
            item_loader.add_value("property_type","apartment")
        description=response.xpath("//div[@class='detail-description']//span/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//picture//img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        furnished=response.xpath("//li[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished",True)
        name=response.xpath("//div[@class='agent-name']/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//p[@class='phone mb-0']/a/i/following-sibling::text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)

        yield item_loader.load_item()