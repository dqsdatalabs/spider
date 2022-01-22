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
class MySpider(Spider):
    name = 'lionsestate_pl'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source = "Lionsestate_PySpider_poland"
    
    headers={
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://lionsestate.pl/wp-json/en/lions_estate/offer/?Offer.order=&Offer.transaction=132&Offer.mainTypeId=2&page=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://lionsestate.pl/wp-json/en/lions_estate/offer/?Offer.order=&Offer.transaction=132&Offer.mainTypeId=1&page=1",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            headers=self.headers,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        total_pages = data["totalPages"]
        for item in data["items"]:
            base_url = "https://lionsestate.pl"
            
            follow_url = base_url + item["url"]
            city = item["locationCityName"]
            if item["locationPrecinctName"]:
                address = item["locationPrecinctName"]
            else:
                address = city

            yield Request(follow_url, headers=self.headers, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), "city":city, "address": address})
            seen = True

        if page == 2 or seen:
            if page <= total_pages:
                f_url = response.url.replace(f"page={page-1}", f"page={page}")
                yield FormRequest(f_url, headers=self.headers, callback=self.parse, meta={"page": page + 1, "property_type": response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)  
        item_loader.add_value("property_type", response.meta.get('property_type'))
                
        external_id = response.xpath("(//li[contains(.,'Nr ofer')]/span/text())[1]").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//div[@class='offer page-content']//div[@class='header']/h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        else:
            title = response.meta.get("city")
            item_loader.add_value("title", title)
        
        rent = response.xpath("//span[@class='full_price']/span/text()[normalize-space()]").get()
        if rent:
            price = rent.split(" PLN")[0].replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "PLN")
        
        room_count = response.xpath("(//li[contains(.,'Sypialnie')]/span/text())[1]").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("(//li[contains(.,'Łazienki')]/span/text())[1]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        square_meters = response.xpath("(//li[contains(.,'Działka')]/span/text())[1]").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
            item_loader.add_value("square_meters", square_meters)
        
        city = response.meta.get("city")
        if city:
            item_loader.add_value("city", city)
        
        address = response.meta.get("address")
        if address:
            item_loader.add_value("address", address)

        parking = response.xpath("//div[@class='key-features']/ul/li/text()[contains(.,'Garaż')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//div[@class='key-features']/ul/li/text()[contains(.,'Parking')]").get()
            if parking:
                item_loader.add_value("parking", True)
        
        balcony = response.xpath("//div[@class='key-features']/ul/li/text()[contains(.,'Balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
               
        latitude = response.xpath("//div[@class='offer-map']/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//div[@class='offer-map']/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        description = " ".join(response.xpath("//div[@class='content-text']/text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        images = [x for x in response.xpath("//div[@class='offer gallery']//a//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Lionsetate")
        item_loader.add_value("landlord_phone", "+48 22 826 66 51")
        item_loader.add_value("landlord_email", "office@lionsestate.pl")
        
        yield item_loader.load_item()