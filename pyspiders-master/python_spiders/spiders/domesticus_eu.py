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
from urllib.parse import urljoin
import re
from datetime import date, datetime
import datetime
class MySpider(Spider):
    name = 'domesticus_eu_disabled'
    execution_type='testing'
    country='netherlands'
    locale='en'
 
    def start_requests(self):
        start_urls = [
            {
                "url" : ["https://digs.one/offers/country_niderlandy?purpose=rent&type=apartments&negotiable=1&notset=1"],
                "property_type" : "apartment"
            },
            {
                "url" : ["https://digs.one/offers/country_niderlandy?purpose=rent&type=houses&negotiable=1&notset=1"],
                "property_type" : "house"
            },
            {
                "url" : ["https://digs.one/offers/country_niderlandy?purpose=rent&type=rooms&negotiable=1&notset=1"],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='ads-container']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
           
        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get('property_type')

        # date_a = "".join(response.xpath("//span[@class='cn-block-label-pnt-value']/text()").getall())
        # today = date.today().strftime("%d.%m.%Y")
        # if date_a and today:
        #     date_a = date_a.strip().replace("\n","").replace("\t","")
        #     d1 = datetime.datetime.strptime(date_a, '%d.%m.%Y')
        #     d2 = datetime.datetime.strptime(today, '%d.%m.%Y')
        #     diff = (d2-d1).days
        #     if diff > 60: #60 days ads.
        #         return
        # else:
        #     return
                
        item_loader.add_value("external_link", response.url) 
        
        item_loader.add_value("external_source", "Domesticus_PySpider_"+ self.country + "_" + self.locale)
        title = response.xpath("//div//h1//text()").get()
        if title:
            item_loader.add_value("title", title) 
            if "studio" in title:
                property_type = "studio"
        item_loader.add_value("property_type", property_type)
        address = response.xpath("//span[contains(.,'Country')]/following-sibling::h3/text()").get()
        if address:
            address = address.strip().strip('the').strip()
            item_loader.add_value("address", address)
            city = address.split(',')[-1].strip()
            if city:
                item_loader.add_value("city", city)
        
        bathroom_count = response.xpath("//h2[contains(.,'Amount of bathroom')]/../h3/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        latitude_longitude = response.xpath("//script[contains(.,'lat =')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat = parseFloat('")[-1].split("'")[0].strip()
            longitude = latitude_longitude.split("lng = parseFloat('")[-1].split("'")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        desc = "".join(response.xpath("//span[@class='addds-text']//text()").extract())
        item_loader.add_value("description", desc.strip())

        if desc:
            if 'deposit' in desc.lower():
                deposit = desc.lower().split('deposit is ')[-1].split('€')[0].strip()
                item_loader.add_value("deposit", deposit)
            if 'no pets' in desc.lower() or  'pets are not allowed' in desc.lower():
                item_loader.add_value("pets_allowed", False)
            if 'elevator' in desc.lower():
                item_loader.add_value("elevator", True)
            if 'swimming pool' in desc.lower():
                item_loader.add_value("swimming_pool", True)

        square_meters = response.xpath("//h2[contains(.,'Living area')]/following-sibling::h3/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip().strip('+')
            item_loader.add_value("square_meters", square_meters)
        else:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2|m^2)",title.replace(",","."))
            if unit_pattern:
                square_meters=unit_pattern[0][0]
            else:
                unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2|m^2)",desc.replace(",","."))
                if unit_pattern:
                    square_meters=unit_pattern[0][0]
            if square_meters:
                item_loader.add_value("square_meters", square_meters)
            elif "Size:" in desc:
                square=desc.split("Size:")[1].split("m^2")[0]
                if square.isdigit():
                    item_loader.add_value("square_meters",square)
        
        room_count = response.xpath("//h2[contains(.,'bedroom')]/following-sibling::h3/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        elif not room_count:
            room_count = response.xpath("//h2[contains(.,'Amount of room')]/following-sibling::h3/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
            else:                
                room_prop = response.xpath("//div[span[contains(.,'Property')]]/h3//text()").get()
                if "Room" in room_prop or "Studio" in room_prop:
                    item_loader.add_value("room_count", "1")
                elif "bedroom" in desc:
                    room=desc.split("bedroom")[0].strip().split(" ")[-1]
                    if room.replace("-","").replace("nd","").replace("(","").isdigit():
                        item_loader.add_value("room_count",room)
                    else:
                        room_c=room_trans(room)
                        if room_c:
                            item_loader.add_value("room_count",room)

        rent = response.xpath("//span[contains(.,'Price')]/following-sibling::span/span/strong/text()").get()
        if rent:
            rent = rent.strip()
            item_loader.add_value("rent", rent)

        currency = response.xpath("//span[contains(.,'Price')]/following-sibling::span/span/text()").get()
        if currency:
            currency = currency.strip()
            if currency == '$':
                currency = 'USD'
            else:
                currency = 'EUR'
            item_loader.add_value("currency", currency)
        else:
            item_loader.add_value("currency", 'EUR')

        

        images = [urljoin('https://www.domesticus.eu', x) for x in response.xpath("//ul[@id='gallery']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        imagescheck=item_loader.get_output_value("images")
        if not imagescheck:
            if response.xpath("//ul[@id='animatedGallery']//li//img/@src").get():
                images=response.xpath("//ul[@id='animatedGallery']//li//img/@src").get()
                if images:
                    item_loader.add_value("images", images)
                    item_loader.add_value("external_images_count", len(images))
 


        energy_label = response.xpath("//h2[contains(.,'Energy Label')]/following-sibling::h3/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            item_loader.add_value("energy_label", energy_label)

        furnished = response.xpath("//h2[contains(.,'Furniture')]").get()
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)

        floor = response.xpath("//h2[contains(.,'Floor:')]/following-sibling::h3/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        parking = response.xpath("//h2[contains(.,'Garage or parking')]/following-sibling::h3/text()").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        balcony = response.xpath("//h2[contains(.,'Open spaces')]/following-sibling::h3/text()").get()
        if balcony:
            if 'balcony' in balcony.lower():
                balcony = True
                item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//h2[contains(.,'Open spaces')]/following-sibling::h3/text()").get()
        if terrace:
            if 'terrace' in terrace.lower():
                terrace = True
                item_loader.add_value("terrace", terrace)

        washing_machine = response.xpath("//h2[contains(.,'Appliances')]/following-sibling::h3/text()").get()
        if washing_machine:
            if 'washing machine' in washing_machine.lower():
                washing_machine = True
                item_loader.add_value("washing_machine", washing_machine)

        dishwasher = response.xpath("//h2[contains(.,'Appliances')]/following-sibling::h3/text()").get()
        if dishwasher:
            if 'dishwasher' in dishwasher.lower():
                dishwasher = True
                item_loader.add_value("dishwasher", dishwasher)

        landlord_name = response.xpath("//p[contains(.,'Contact name')]/following-sibling::b/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//p[contains(.,'Contact Phone')]/following-sibling::b/a/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)


        yield item_loader.load_item() 

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data 

def room_trans(room):
    if "first" in room.lower() or "single" in room.lower() or "one" in room.lower() or room.lower()=="a":
        return "1"
    if "two" in room.lower() or "double" in room.lower():
        return "2"
    if "three" in room.lower():
        return "3"
    else: return False
        
          

        

      
     