# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
from urllib.parse import urljoin
import re

class MySpider(Spider):
    name = 'villasolrealestate_comenrentals'
    execution_type='testing'
    country='spain'
    locale='en'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.villasolrealestate.com/en/rentals"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='ic-viewMore']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        pagination = response.xpath("//li[@class='item next']/a/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Villasolrealestate_PySpider_"+ self.country + "_" + self.locale)

        
        address=response.xpath("//address//text()").get()
        if address:
            item_loader.add_value("address", address)
            
        description = response.xpath("//div[@id='ic-description']/p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)
        
        if "furnished" in desc_html.lower() and "unfurnished" not in desc_html.lower():
            item_loader.add_value("furnished", True)
        if "lift" in desc_html.lower() and "no lift" not in desc_html.lower():
            item_loader.add_value("elevator", True)
        if "swimming pool" in desc_html.lower():
            item_loader.add_value("swimming_pool", True)
            
        square_meters = response.xpath("//dt[contains(.,'Built size')]/following-sibling::dd/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            if square_meters != '0':
                item_loader.add_value("square_meters", square_meters)
            else:
                square_meters=response.xpath("//dt[ contains(.,'Plot size')]/following-sibling::dd/text()").get()
                square_meters = square_meters.split('m')[0].strip()
                if square_meters != '0':
                    item_loader.add_value("square_meters", square_meters)


        rent = response.xpath("//dt[contains(.,'Price')]/following-sibling::dd/span/text()").get()
        if rent:
            price = rent.split('€')[0].strip().replace(',', '')
            if price != '0':
                item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        external_id = response.xpath("//dt[contains(.,'Reference')]/following-sibling::dd/span/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        city = response.xpath("//dt[contains(.,'Area')]/following-sibling::dd/text()").get()
        if city:
            city = city.split('(')[1].split(')')[0].strip()
            item_loader.add_value("city", city)

        images = [urljoin('https://www.villasolrealestate.com', x) for x in response.xpath("//div[@class='mainImg']/following-sibling::ul/li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        energy_label = response.xpath("//img[@id='imgEnergyPerformance']/@src").get()
        if energy_label and 'No_Data' not in energy_label:
                energy_label = energy_label.split('/')[-1].split('-')[1][0]
                item_loader.add_value("energy_label", energy_label)

        parking = response.xpath("//ul[@class='ic-filled ic-horiz']/li[contains(.,'Garage')]").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)
        else:
            parking = response.xpath("//dt[contains(.,'Parking') or contains(.,'Garage')]/following-sibling::dd/text()").get()
            if parking and "no" not in parking.lower():
                item_loader.add_value("parking", True)

        balcony = response.xpath("//ul[@class='ic-filled ic-horiz']/li[contains(.,'Balcony')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//ul[@class='ic-filled ic-horiz']/li[contains(.,'Terrace')]").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        swimming_pool = response.xpath("//ul[@class='ic-filled ic-horiz']/li[contains(.,'Swimming Pool')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        washing_machine = response.xpath("//ul[@class='ic-filled ic-horiz']/li[contains(.,'Washing Machine')]").get()
        if washing_machine:
            washing_machine = True
            item_loader.add_value("washing_machine", washing_machine)

        dishwasher = response.xpath("//ul[@class='ic-filled ic-horiz']/li[contains(.,'Dishwasher')]").get()
        if dishwasher:
            dishwasher = True
            item_loader.add_value("dishwasher", dishwasher)

        landlord_phone = response.xpath("//p[@class='ic-faphone']/a/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//a[contains(@href,'mail')]/span/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        item_loader.add_value("landlord_name", "VILLASOL REAL ESTATE")
        
        bathroom_count=response.xpath("//dt[contains(.,'Bathroom')]/following-sibling::dd/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        prop_type=response.xpath("//ul/li/dt[contains(.,'Property')]/following-sibling::dd/text()").get()
        room_count = response.xpath("//dt[contains(.,'Bedrooms')]/following-sibling::dd/text()").get()
        
        if "studio" in prop_type.lower():
            item_loader.add_value("room_count","1")        
        elif room_count:
            room_count = room_count.strip()
            if room_count != '0':
                item_loader.add_value("room_count", room_count)
        
        property_type=""
        list_status=["villa","house","cortijo","studio","country"]
        if "apartment" in prop_type.lower():
            property_type="apartment"
        elif "studio" in prop_type.lower():
            property_type="studio"
        else:
            for i in range(0,len(list_status)):
                if list_status[i] in prop_type.lower():
                    property_type="house"
                    break
        
        if property_type:
            item_loader.add_value("property_type", property_type)         
            yield item_loader.load_item()
        

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data