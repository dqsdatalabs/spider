# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'century21_lb_maisons_alfort_com'
    execution_type='testing'
    country='france'
    locale='fr'

    custom_settings = {
        "CONCURRENT_REQUESTS" : 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
        "PROXY_ON" : True,
    }

    def start_requests(self):
        start_urls = [
            {"url": "https://www.century21-lb-maisons-alfort.com/annonces/location-appartement/", "property_type": "apartment"},
	        {"url": "https://www.century21-lb-maisons-alfort.com/annonces/location-maison/", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='js-the-list-of-properties-list-property']"):
            follow_url = response.urljoin(item.xpath(".//a[@title='Voir le détail du bien']/@href").extract_first())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        title = " ".join(response.xpath("//h1/span//text()[normalize-space()]").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        address = " ".join(response.xpath("//h1/span[last()]/text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(" -")[0])
        zipcode = response.xpath("//li[@itemprop='itemListElement'][last()]//span[@itemprop='name']/text()").get()
        if zipcode: 
            item_loader.add_value("zipcode", zipcode.split('(')[-1].split(')')[0].strip())
          

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Century21_Lb_Maisons_Alfort_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//section[@class='c-the-property-detail-description']/div//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li[contains(.,'Surface habitable')]/text()[normalize-space()]", input_type="F_XPATH", get_num=True, split_list={":":1, "m":0, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'de pièce')]/text()[normalize-space()]", input_type="F_XPATH", get_num=True, split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[contains(@class,'property-abstract__price')]/text()", input_type="M_XPATH", get_num=True, split_list={"/":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'de garantie')]/text()[normalize-space()]", input_type="F_XPATH", get_num=True, split_list={":":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'c-the-detail-images__item')]//img/@src[not(contains(.,'/theme/v6/'))]", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div[contains(@class,'c-the-map-of-a-property')]/@data-lat", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div[contains(@class,'c-the-map-of-a-property')]/@data-lng", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//p[contains(.,'provision pour charges')]/text()[normalize-space()]", input_type="F_XPATH", get_num=True, split_list={":":-1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="CENTURY 21 LB Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01 56 29 21 21", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[contains(.,'ref')]/text()[normalize-space()]", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Ascenseur')]/text()[normalize-space()]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon')]/text()[normalize-space()]", input_type="F_XPATH", tf_item=True)
        
        energy_label = response.xpath("//div[p[contains(.,'nergie')]]//span[@class='tw-block tw-m-auto']/text()").get()
        if energy_label:
            energy_label = energy_label.split(",")[0]
            if int(energy_label) <= 50: item_loader.add_value("energy_label", "A")
            elif int(energy_label) > 50 and int(energy_label) <= 90: item_loader.add_value("energy_label", "B")
            elif int(energy_label) > 90 and int(energy_label) <= 150: item_loader.add_value("energy_label", "C")
            elif int(energy_label) > 150 and int(energy_label) <= 230: item_loader.add_value("energy_label", "D")
            elif int(energy_label) > 230 and int(energy_label) <= 330: item_loader.add_value("energy_label", "E")
            elif int(energy_label) > 330 and int(energy_label) <= 450: item_loader.add_value("energy_label", "F")
            elif int(energy_label) > 450: item_loader.add_value("energy_label", "G")
        
         
        yield item_loader.load_item()

