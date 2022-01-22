# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
from html.parser import HTMLParser

class MySpider(Spider):
    name = 'fleximmo_net'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.fleximmo.net/location"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@id='list-result']/li"):
            follow_url = response.urljoin(item.xpath(".//div[@class='produit']/a/@href").get())
            prop_type = item.xpath(".//h3[@class='title']/text()").get()
            property_type = ""
            if "appartement" in prop_type.lower():
                property_type = "apartment"
            elif "maison" in prop_type.lower():
                property_type = "house"
            elif "studio" in prop_type.lower():
                property_type = "apartment"
            elif "duplex" in prop_type.lower():
                property_type = "apartment"
            elif "villa" in prop_type.lower():
                property_type = "house"
            if property_type != "":
                yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_xpath("title", "//div[contains(@class,'detail-visuel-wrapper')]//h2[@class='subtitle']/text()")
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Fleximmo_PySpider_"+ self.country + "_" + self.locale)

        address = response.xpath("//input[@id='address']/@value").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'setView')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('setView([')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('setView([')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        square_meters = response.xpath("//span[@class='item pieces']/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split(',')[1].split('m')[0].strip().replace(',', '.'))))
            if square_meters != '0':
                item_loader.add_value("square_meters", square_meters)

        room = response.xpath("//span[contains(@class,'pieces')]/text()").get()
        if room:
            room = room.split("pièce")[0].strip()
            item_loader.add_value("room_count", room)

        rent = response.xpath("//span[@class='top-price']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace('\xa0', '').replace(' ', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        external_id = response.url.split(',')[-1]
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//h2[contains(.,'Descriptif')]/following-sibling::p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '').replace('\r', '').replace('\n', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        images = [x for x in response.xpath("//div[@id='container-visuels']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        if "de garantie" in desc_html:
            deposit = desc_html.split("de garantie")[1].split("\u20ac")[0].replace(":","").strip()
            item_loader.add_value("deposit", deposit)
        
        if "charge locataire" in desc_html:
            utilities = desc_html.split("charge locataire")[1].split("\u20ac")[0].replace(":","").strip()
            item_loader.add_value("utilities", utilities)
            
        
        # deposit = response.xpath("//br[contains(following-sibling::text(), 'Dépôt de garantie')]/following-sibling::text()[1]").get()
        # if deposit:
        #     deposit = deposit.split('€')[0].strip().replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '')
        #     item_loader.add_value("deposit", deposit)

        energy_label = response.xpath("//img[contains(@src,'dpe')]/@src").get()
        if energy_label:
            energy_label = energy_label.split('_')[-1].split('.')[0].strip().upper()
            item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", "FLEXIMMO AGENCE IMMOBILIERE")
        item_loader.add_value("landlord_phone", "03 89 31 97 40")
        item_loader.add_value("landlord_email", "contact@fleximmo.eu")
        
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data