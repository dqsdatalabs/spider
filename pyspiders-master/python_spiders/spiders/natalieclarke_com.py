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
import re
from geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'natalieclarke_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'   
    external_source='Natalieclarke_PySpider_united_kingdom'
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.natalieclarke.com/propertysearch/24048/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.natalieclarke.com/propertysearch/24053/",
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

        for item in response.xpath("//div[@class='property-list']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)  
        item_loader.add_value("external_id", response.url.split("/")[-3])        
        item_loader.add_xpath("room_count", "//span[contains(.,'Bedroom')]/following-sibling::span/text()")
        address = " ".join(response.xpath("//h1//text()[normalize-space()]").getall())
        if address:
            address = re.sub(r'\s{2,}', '', address.strip())
            item_loader.add_value("address", address)
            item_loader.add_value("title", address.replace("\n","").strip())
            zipcode = address.split(",")[-1]
            if zipcode:
                item_loader.add_value("zipcode", zipcode.strip())
            city = address.split(",")[-2]
            if city:
                item_loader.add_value("city", city.strip())

            latitude_longitude = response.xpath("//script[contains(.,'myLatLng')]//text()").get()
            if latitude_longitude:
                latitude = latitude_longitude.split('myLatLng = {lat:')[1].split(',')[0].strip()
                longitude = latitude_longitude.split(', lng:')[1].split('}')[0].strip()
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

        available_date = response.xpath("//span[contains(.,'Available')]/following-sibling::span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['fr'])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        rent = response.xpath("//span[contains(.,'£')]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
     
        energy_label = response.xpath("//a[contains(@href,'test-popup')]/text()").re_first(r"\D")
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
  
        
        description = " ".join(response.xpath("//div[@class='prop-dets-content']//text()").getall()) 
        if description:
            item_loader.add_value("description", re.sub(r'\s{2,}','',description))

            if "parking" in description.lower():
                item_loader.add_value("parking", True)
    
        furnished = response.xpath("//span[contains(.,'Furnished')][2]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)        
       
        images = [x for x in response.xpath("//ul[@id='gallery']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "Natalie Clarke Residential")
        item_loader.add_value("landlord_phone", "028 9031 0500")
        item_loader.add_value("landlord_email", "hello@natalieclarke.com")
 
        yield item_loader.load_item()