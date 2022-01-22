# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


import itemadapter
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import re
import dateparser
from datetime import datetime

class MySpider(Spider):
    name = 'immo_wilink_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source="Immo_Wilink_PySpider_belgium"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }
    headers={
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Host": "www.wilinkrealestate.be.",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
    }
    formdata={
        "form_build_id": "form-a-xKvTMSozs2aLZMdqZ4q1OMdNjWBqwWXwvm1KlJiGU",
        "form_id": "huren_filter_form",
        "access_kye": "",
        "PurposeStatusIds": "2",
        "City": "",
        "price_min": "",
        "price_max": "",
        "Rooms": "",
        "sort": "false",
        "_triggering_element_name": "next",
        "_triggering_element_value": "Volgende",
        "_drupal_ajax": "1",
        "ajax_page_state[theme]": "wilink",
        "ajax_page_state[theme_token]": "",
        "ajax_page_state[libraries]": "bootstrap4/bootstrap4-js-latest,bootstrap4/global-styling,classy/base,classy/messages,core/normalize,eu_cookie_compliance/eu_cookie_compliance_default,fontawesome/fontawesome.webfonts.shim,properties/properties,system/base,wilink/global-styling"
    }
    def start_requests(self):
        start_urls = [
            {"url": "http://www.wilinkrealestate.be./huren"},
        ]  # LEVEL 1
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,headers=self.headers)

    # 1. FOLLOWING 
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        if "ajax" in response.url:
            data=str(response.body).split('"data":"')[-1].split('","settings')[0].replace("\\u003C","").replace("\\u0022","")
            if data:
                for item in data.split('class=\col-sm-4 mb-4 px-2'):
                    item=item.split("class=\card h-100")[0].split("a href=")[-1].split("u003E")[0].replace("\\","")
                    follow_url=response.urljoin(item)
                    yield Request(follow_url, callback=self.populate_item)
        for item in response.xpath("//div[@class='col-sm-4 mb-4 px-2']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        if page == 2 or seen:
            url = "http://www.wilinkrealestate.be./huren?ajax_form=1&_wrapper_format=drupal_ajax"
            yield FormRequest(url, callback=self.parse,formdata=self.formdata)

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")
        property_type=response.xpath("//td[.='Type']/following-sibling::td/text()").get()
        if property_type and "appartement"==property_type.lower():
            item_loader.add_value("property_type","apartment")

        studio = response.xpath("//div[@class='title']/h1/text()").extract_first()
        if studio == "Studio":
            item_loader.add_value("property_type", "studio") 

        rent = response.xpath("//p[.='PRIJS']/following-sibling::h2/text()").get()
        if rent:
            price = rent.split("maand")[0].split("€")[1].split(",")[0]
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        address = response.xpath("//div[@class='address py-2']/p/text()").get()
        if address:
            item_loader.add_value("address", address)     
        city = response.xpath("//div[@class='address py-2']/p/text()").get()
        if city:
            item_loader.add_value("city", city.split(",")[-1].strip().split(" ")[1])
            item_loader.add_value("zipcode", city.split(",")[-1].strip().split(" ")[0])
        
        energy_label=response.xpath("//td[.='EPC-klasse']/following-sibling::td/text()").get()
        if energy_label:
           item_loader.add_value("energy_label",energy_label)
        square_meters = response.xpath("//sup[.='2']/preceding-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        room_count = response.xpath("//p[contains(.,'slaapkamers')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        bathroom_count = response.xpath("//p[contains(.,'badkamer')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
        floor = response.xpath("//td[contains(.,'verdieping')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        desc = "".join(response.xpath("//div[@class='description pb-4 pt-5']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        images = [ x for x in response.xpath("//a[@class='item']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        elevator=response.xpath("//td[.='lift']/following-sibling::td/text()").get()
        if elevator and "Ja"==elevator:
            item_loader.add_value("elevator",True)
        
        name = response.xpath("//p[@class='font-weight-bold title']//text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        phone = response.xpath("//a[contains(@href,'tel')]//text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)

        yield item_loader.load_item()