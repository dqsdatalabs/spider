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
from python_spiders.helper import ItemClear
from word2number import w2n

class MySpider(Spider):
    name = 'edouard7_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self): 
        start_urls = [
            {
                "url" : [
                    "https://www.edouard7.com/fr/resultats?ref=&status=124&ptype=120&city=&minPrice=&maxPrice=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.edouard7.com/fr/resultats?ref=&status=124&ptype=123&city=&minPrice=&maxPrice=",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//h4/a[@target='_blank']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Edouard7_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='jv-custom-post-content']//h2/text()", input_type="F_XPATH", split_list={" villa ":-1,"partement":-1," louer ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='jv-custom-post-content']//h2/text()", input_type="F_XPATH", split_list={" villa ":-1,"partement":-1," louer ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//span[@class='jv-listing-title']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='jv-custom-post-content']/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[p[.='Surface M²']]/span/text()", input_type="F_XPATH", get_num=True, split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//li[@class='javo-single-nav price']/a/text()[contains(.,'€')]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p[label[.='Dépôt de garantie']]/text()[normalize-space()]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='row']//a[@class='link-display']/img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[@id='lava-realestate-manager-lava-single-js-js-extra']/text()[contains(.,'lng')]", input_type="F_XPATH", split_list={'"lat":':1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[@id='lava-realestate-manager-lava-single-js-js-extra']/text()[contains(.,'lng')]", input_type="F_XPATH", split_list={'"lng":':1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@id='lava-realestate-amenities']/div[contains(.,'Parking')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[@id='lava-realestate-amenities']/div[contains(.,'Ascenseur')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@id='lava-realestate-amenities']/div[contains(.,'Terrasse')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@id='lava-realestate-amenities']/div[contains(.,'Balcon')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//div[@id='lava-realestate-amenities']/div[contains(.,'Piscine')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//p[label[contains(.,'Meublé')]][i[@class='fa fa-check-square-o']]//text()[normalize-space()] | //span[contains(.,' Meublé')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p//text()[contains(.,'de provision')]", input_type="F_XPATH", get_num=True, split_list={"dont":1, "€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@id='lv-single-contact']//li[contains(.,'Direct')]/div/a//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@id='lv-single-contact']//li[contains(.,'Téléphone')]/div[2]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="contact@edouard7.com", input_type="VALUE")
        room_count = response.xpath("//div[@class='jv-custom-post-content']/p//text()[contains(.,'chambre')]").get()
        if room_count:
            room = room_count.split("chambre")[0].strip().split(" ")[-1]
            if room.isdigit():
                item_loader.add_value("room_count", room)
            else:
                room = room_count.split("pièce")[0].strip().split(" ")[-1].replace("),","")
                if "deux" in room.lower():
                    item_loader.add_value("room_count", "2")
                elif "trois" in room_count.lower():
                    item_loader.add_value("room_count", "3")
            
        elif not room_count:
            room_count = response.xpath("//title/text()").get()
            if room_count:
                index=room_count.find("petit deux pièces")
                if index:
                    item_loader.add_value("room_count", "2")
                    

        elif "studio" in response.url:
            item_loader.add_value("room_count", "1")

        energy_label = response.xpath("//p[label[contains(.,'performance énergétique')]]/text()[normalize-space()]").get()
        if energy_label:
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