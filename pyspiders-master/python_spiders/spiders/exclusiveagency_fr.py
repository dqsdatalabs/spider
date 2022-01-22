# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'exclusiveagency_fr'
    execution_type='testing'
    country='france'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        start_urls = [
            {"url": "http://www.exclusiveagency.fr/en/rentals/long-term-rentals"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
      
        for item in response.xpath("//ul[@class='ads']/li/a/@href").extract():
            follow_url = response.urljoin(item)
            if "house" in follow_url:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": "house"})
                seen = True
            elif "apartment" in follow_url:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": "apartment"})
                seen = True
        next_page = response.xpath("//li[@class='nextpage']/a/@href").get()
        if next_page:
            url = response.urljoin(next_page)
            yield Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Exclusiveagency_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//div[@class='title']/h1/text()")
        item_loader.add_value("external_link", response.url)

        external_id = response.xpath("//p[@class='comment']/span/text()").get()
        if external_id:
            external_id = external_id.strip().split(' ')[1]
            item_loader.add_value("external_id", external_id)

        data = response.xpath("//script[contains(., 'L.marker')]").get()
        latitude = ''
        longtitude = ''
        if data:
            latlong = data.split('L.marker([')[1].split(']')[0].strip()
            latitude = latlong.split(',')[0].strip()
            longtitude = latlong.split(',')[1].strip()       
            item_loader.add_value("longitude", longtitude)
            item_loader.add_value("latitude", latitude)

        address=response.xpath("//div[@class='path']/p/span/text()").get()
        if address:
            address=address.replace("House","").replace("Apartment","").strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        item_loader.add_value("property_type", response.meta.get("property_type"))

        square_meters = response.xpath("//li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(' ')[0]
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//li[contains(.,'Rooms')]/span/text()").get()
        if room_count:
            if len(room_count.split(' ')) > 1:
                room_count = room_count.split(' ')[0].strip()
            else:
                room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count=response.xpath("//ul/li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(' ')[0])
        
        price = response.xpath("//div[@class='title']/h2/text()[.!='Price on request']").get()
        if price: 
            if "Month" in price:
                item_loader.add_value("rent_string",price)
            else:
                price2 = price.replace(",","").split("€")[0].replace("From","")
                item_loader.add_value("rent",int(price2)*4)
                
            item_loader.add_value("currency", "EUR")

        desc="".join(response.xpath("//p[@class='comment']/text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        item_loader.add_value("description", desc)

        images = [x for x in response.xpath("//aside[@class='showThumbs']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        floor = response.xpath("//li[contains(.,'Floor')]/span/text()").get()
        if floor:
            if "rd" in floor:
                floor = floor.split('rd')[0].strip()
            elif "/" in floor:
                floor = floor.split('/')[1].strip().split(" ")[0].strip()
            item_loader.add_value("floor", floor)
        
        deposit=response.xpath("//div[@class='legal']/ul/li[contains(.,'Guarantee')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].replace(",","").strip())
        
        washing_machine=response.xpath("//div[@class='services']/ul//li[contains(.,'Washing')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        elif "washing machine" in desc.lower():
            item_loader.add_value("washing_machine", True)
        
        energy_l=response.xpath("//div[contains(@class,'diagnostics')]/img/@src").get()
        if energy_l:
            energy_l=energy_l.strip().split("/")[-1]
            if energy_label(energy_l):
                item_loader.add_value("energy_label", energy_label(energy_l))
                
        swimming_pool = response.xpath("//div[@class='services']/ul//li[.='Swimming pool']/text()").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        furnished = response.xpath("//div[@class='services']/ul//li[.='Furnished']/text()").get()
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)
        
        terrace = response.xpath("//div[@class='areas']/ul//li[contains(.,'Terrace')]/text()").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        landlord_name = response.xpath("//aside[@class='map']/parent::div/aside[1]/p[4]/strong/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//aside[@class='map']/parent::div/aside[1]/p[4]/span[2]/a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)

        landlord_phone = response.xpath("//aside[@class='map']/parent::div/aside[1]/p[4]/span[1]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data

def energy_label(energy_label):
    if int(energy_label)==-1:
        energy_l=False
    elif int(energy_label)<51:
        energy_l="A"
    elif int(energy_label)<91:
        energy_l="B"
    elif int(energy_label)<151:
        energy_l="C"
    elif int(energy_label)<231:
        energy_l="D"
    elif int(energy_label)<331:
        energy_l="E"
    elif int(energy_label)>450:
        energy_l="F"
    
    return energy_l