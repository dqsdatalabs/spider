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
from  geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'finrealestates_com'
    execution_type='testing'
    country='turkey'
    locale='en'
    thousand_seperator=','
    
    custom_settings = {
        "HTTPCACHE_ENABLED": True
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url" : ["https://www.finrealestates.com/apartment-for-rent-istanbul/?tab=for-rent"],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.finrealestates.com/houses-rent-istanbul/?tab=for-rent",
                    "https://www.finrealestates.com/villas-rent-istanbul/?tab=for-rent",
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

        for item in response.xpath("//figure[@class='item-thumb']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
    
        next_page = response.xpath("//a[@rel='Next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        with open("debug", "wb") as f:f.write(response.body)
        item_loader.add_value("external_source", "Finrealestates_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        commercial = response.xpath("//ol[@class='breadcrumb']//li//a/span[.='Commercial']/text()").get()
        if commercial:
            return
        
        title = response.xpath("//div[@id='description']/h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        rent=response.xpath("//ul/li[contains(.,'Price')]/text()[contains(.,'₺')]").get()
        if rent:
            item_loader.add_value("rent", str(rent.split('₺')[1].replace(',','.')))
            item_loader.add_value("currency", "TRY")
        
        square_meters=response.xpath("//ul/li[contains(.,'Size')]/text()[contains(.,'m2')]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('-')[0].strip())
        
        room_count=response.xpath("//ul/li[contains(.,'Bedrooms')]/text()[contains(.,'-')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split('-')[1].strip())
        
        bathroom_count=response.xpath("//ul/li[contains(.,'Bathroom')]/text()[contains(.,'-')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split('-')[1].strip())
        
        address=response.xpath("//ul/li[contains(.,'Address')]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        city=response.xpath("//ul/li[contains(.,'City')]/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        
        zipcode=response.xpath("//ul/li[contains(.,'Zip')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        
        latitude_longitude = response.xpath("//script[contains(.,'property_lat')]/text()").get()
        if latitude_longitude:
                latitude = latitude_longitude.split('property_lat":"')[1].split('"')[0]
                longitude = latitude_longitude.split('property_lng":"')[1].split('"')[0]
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)

        desc="".join(response.xpath("//div[@id='description']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//div[@class='item']/img/@data-src | //div[@id='gallery']/@nitro-lazy-bg").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name","FIN REAL ESTATE")
        item_loader.add_value("landlord_phone","90 212 90 91 908")
        item_loader.add_value("landlord_email","info@finrealestates.com")
            
        balcony=response.xpath("//div[@id='features']/ul/li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator=response.xpath("//div[@id='features']/ul/li[contains(.,'Elevator')]//text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
            
        parking=response.xpath("//div[@id='features']/ul/li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking",True)
            
        swimming_pool=response.xpath("//div[@id='features']/ul/li[contains(.,'Pool')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool",True)
        
        external_id = response.xpath("substring-after(//link[@rel='shortlink']/@href,'=')").get()
        item_loader.add_value("external_id", external_id)
        
        
        yield item_loader.load_item()

        
       

        
        
          

        

      
     