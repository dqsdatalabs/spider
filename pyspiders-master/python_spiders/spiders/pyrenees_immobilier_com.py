# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'pyrenees_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Pyrenees_Immobilier_PySpider_france"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.pyrenees-immobilier.com/ajax/ListeBien.php?page={}&RgpdConsent=1614082586489&ListeViewBienForm=pict&ope=2&filtre=2&langue=fr&MapWidth=100&MapHeight=394&DataConfig=JsConfig.GGMap.Liste&Pagination=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.pyrenees-immobilier.com/ajax/ListeBien.php?page={}&RgpdConsent=1614082586489&ListeViewBienForm=pict&ope=2&filtre=182&langue=fr&MapWidth=100&MapHeight=394&DataConfig=JsConfig.GGMap.Liste&Pagination=0", 
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"): 
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        total_page = response.xpath("//span[contains(@class,'nav-page-position')]/text()").get()
        if total_page:
            total_page = int(total_page.split("/")[-1].strip())
        else:
            total_page = 1
        for item in response.xpath("//div[contains(@class,'liste-bien-photo')]/div[1]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page <= total_page:
            base = response.meta["base"]
            p_url = base.format(page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "base":base, "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        dontallow=response.xpath("//span[.='Type']/following-sibling::text()").get()
        if dontallow and "parking" in dontallow.lower():
            return 
        parking_type = response.xpath("//div[@class='detail-bien-specs']//li[span[.='Type']]/text()[.=' Parking intérieur']").get()
        if parking_type:
            return
        item_loader.add_value("external_source",self.external_source)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='detail-bien-specs']//li[span[.='Ref']][2]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//meta[@property='og:title']/@content", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='detail-bien-specs']//li[span[.='Ville']]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='detail-bien-specs']//li[span[.='Ville']]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//li[@class='gg-map-marker-lat']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//li[@class='gg-map-marker-lng']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='detail-bien-desc-content']/p[not(contains(.,' baisse de prix'))]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='detail-bien-specs']//li[span[.='Surface']]/text()", input_type="F_XPATH", get_num=True, split_list={"m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li[@class='prix']//text()", input_type="M_XPATH", get_num=True, replace_list={" ":""},split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li/span[contains(.,'de garantie')]/following-sibling::span[1]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li/i/span[contains(.,'sur charges')]/following-sibling::span[1]/text()[.!='0']", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='big-flap-container']/div/img/@data-src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Agence Pyrenees Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="05.34.02.10.41", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="pamiers@pyrenees-immobilier.com", input_type="VALUE")    
        room_count = response.xpath("//div[@class='detail-bien-specs']//li[span[.='Pièces']]/text()[.!=' NC']").get()
        if room_count: 
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//div[@class='detail-bien-specs']//li[span[.='Pièces']]/text()[.!=' NC']")
        bathroom_count = response.xpath("//div[@class='detail-bien-desc-content']/p//text()[contains(.,'salle d')]").get()
        if bathroom_count:        
            bathroom_count = bathroom_count.split(" salle d")[0].strip().split(" ")[-1].strip()
            if "une" in bathroom_count:
                item_loader.add_value("bathroom_count", "1")
            elif "deux" in bathroom_count:        
                item_loader.add_value("bathroom_count", "2")  
            elif "trois" in bathroom_count:
                item_loader.add_value("bathroom_count", "3")
        energy = response.xpath("//div[@id='Dpe']/img[contains(@src,'nrj')]/@src").get()
        if energy:
            energy_label = energy.split('-')[-1].split(".")[0].strip()
            if energy_label.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy_label))
        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label