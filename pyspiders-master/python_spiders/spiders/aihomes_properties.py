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
import re


class MySpider(Spider):
    name = 'aihomes_properties'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : "http://aihomes.properties/letlist.aspx?lang=en",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='caption-2']/h3/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        
        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value('external_source', 'Aihomes_PySpider_united_kingdom')

        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_id", response.url.split("=")[-1].strip())
        address = " ".join(response.xpath("//tr[td[.='Address']]/td[2]//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address)
            
            city = address.split(",")[-1].strip()
            if city.count(" ") ==1:
                item_loader.add_value("zipcode",address.split(",")[-1].strip())
                item_loader.add_value("city", address.split(",")[-2].strip())
            if city.count(" ") >=2:
                item_loader.add_value("zipcode", " ".join(city.split(" ")[-2:]))
                item_loader.add_value("city", " ".join(city.split(" ")[:-2]))
        rent = " ".join(response.xpath("//div[@class='cell-md-4']/p/strong/span/text()").getall())
        if rent:
            price = rent.split(" ")[0].strip().replace(",","").strip()
            item_loader.add_value("rent_string", price)

        meters = " ".join(response.xpath("//tr[td[.='Area']]/td[2]//text()").getall())
        if meters:
            item_loader.add_value("square_meters", meters.split(" ")[0].strip())

        room_count = " ".join(response.xpath("//tr[td[.='Bedrooms']]/td[2]//text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.split("bedrooms")[0].strip())
            item_loader.add_value("bathroom_count", room_count.split("bathrooms")[0].split(",")[1].strip())

        desc = "".join(response.xpath("//div[@class='cell-md-8']/p//text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        images = [response.urljoin(x)for x in response.xpath("//div[@class='cell-md-8']/div/div//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        parking = "".join(response.xpath("//tr[td[.='Parking']]/td[2]//text()").extract())
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//tr[td[.='Others']]/td[2]//text()").get()
        if balcony and "balcon" in balcony.lower():
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//tr[td[.='Furniture']]/td[2]//text()").get()
        if furnished and "furnished" in furnished.lower() and "un" not in furnished.lower() :
            item_loader.add_value("furnished", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'Location(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('Location(')[1].split(',')[0]
            longitude = latitude_longitude.split('Location(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value('landlord_name', 'AiHome')
        item_loader.add_value('landlord_email', 'info@aihomes.uk')
        item_loader.add_value('landlord_phone', '+44(0) 161 8776795')
        

        yield item_loader.load_item()