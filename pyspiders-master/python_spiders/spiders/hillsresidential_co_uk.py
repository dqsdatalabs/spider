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
from word2number import w2n
import dateparser
from geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'hillsresidential_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.hills.agency/property-search/?action=search&type=property&page=1&sort=price-highest&per-page=12&view=list&tenure=lettings&location=&radius=30&sales-price-min=0&lettings-price-min=0&sales-price-max=999999999999&lettings-price-max=999999999999&bedrooms-min=0&property-type=apartment-studio&amenities%5B%5D=none&undefined=",
                    "https://www.hills.agency/property-search/?action=search&type=property&page=1&sort=price-highest&per-page=12&view=list&tenure=lettings&location=&radius=30&sales-price-min=0&lettings-price-min=0&sales-price-max=999999999999&lettings-price-max=999999999999&bedrooms-min=0&property-type=flat&amenities%5B%5D=none&undefined="
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.hills.agency/property-search/?action=search&type=property&page=1&sort=price-highest&per-page=12&view=list&tenure=lettings&location=&radius=30&sales-price-min=0&lettings-price-min=0&sales-price-max=999999999999&lettings-price-max=999999999999&bedrooms-min=0&property-type=house&amenities%5B%5D=none&undefined=",
                    "https://www.hills.agency/property-search/?action=search&type=property&page=1&sort=price-highest&per-page=12&view=list&tenure=lettings&location=&radius=30&sales-price-min=0&lettings-price-min=0&sales-price-max=999999999999&lettings-price-max=999999999999&bedrooms-min=0&property-type=bungalow&amenities%5B%5D=none&undefined="
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

        for item in response.xpath("//article[@class='component property property--push property--list']/a/@href").extract():            
            yield Request(item, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Hillsresidential_PySpider_"+ self.country + "_" + self.locale)

        external_id = response.url.split('-')[-1].strip('/')
        if external_id:
            item_loader.add_value("external_id", external_id)

        address = response.xpath("//p[contains(@class,'address')]/text()").get()
        if address:
            item_loader.add_value("address", address)
            city = address.split(",")[1].strip()
            if city:
                item_loader.add_value("city", city)
            zipcode = address.split(",")[-1].strip()
            if zipcode:
                item_loader.add_value("zipcode", zipcode)

            geolocator = Nominatim(user_agent=response.url)
            location = geolocator.geocode(address)
            if location:
                lat, log = (location.latitude, location.longitude)
                item_loader.add_value("latitude", str(lat))
                item_loader.add_value("longitude", str(log))

        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//h2[contains(.,'description')]/following-sibling::div/p/text()").getall()).strip()
        if description:
            item_loader.add_value("description", description)
            if 'terrace' in description.lower():
                item_loader.add_value("terrace", True)

            if 'sq.ft' in description.lower():
                square_meters = "".join(filter(str.isnumeric, description.lower().split('sq.ft')[0].strip().split(' ')[-1]))
                item_loader.add_value("square_meters", square_meters)
            if 'floor' in description.lower():
                parsed_text = description.lower().split('floor')
                for i in range(len(parsed_text) - 1):
                    floor = "".join(filter(str.isnumeric, parsed_text[i].strip().split(' ')[-1]))
                    if floor.strip().isnumeric():
                        item_loader.add_value("floor", floor.strip())
                        break

            if 'washing machine' in description.lower():
                item_loader.add_value("washing_machine", True)
            if 'parking' in description.lower():
                item_loader.add_value("parking", True)
            if 'balcony' in description.lower():
                item_loader.add_value("balcony", True)
            if 'furnished' in description.lower():
                item_loader.add_value("furnished", True)
            if 'dishwasher' in description.lower():
                item_loader.add_value("dishwasher", True)

        room_count = response.xpath("//small[contains(.,'Bedroom')]/text()").re_first(r"\d")
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//small[contains(.,'Bathroom')]/text()").re_first(r"\d")
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = "".join(response.xpath("//h4[@class='h2 text-primary']/text()").get())
        if rent:
            rent = rent.replace(",","").split("€")[0]
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'GBP')

        images = [x for x in response.xpath("//a[contains(@href, 'images')]/img/@src").getall() if 'gif' not in x]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        item_loader.add_value("landlord_name", 'Hills Residential')
        item_loader.add_value("landlord_phone", '(0161) 7479379')
        item_loader.add_value("landlord_email", 'lettings@hills.agency')
    
        # available_date = response.xpath("//li[@class='property-bullet' and contains(.,'Available now')]").get()
        # if available_date:
        #     date_parsed = dateparser.parse('now', date_formats=["%d %B %Y"], languages=['en'])
        #     if date_parsed:
        #         date2 = date_parsed.strftime("%Y-%m-%d")
        #         item_loader.add_value("available_date", date2)

        yield item_loader.load_item()
