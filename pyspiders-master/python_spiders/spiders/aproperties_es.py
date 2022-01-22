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
    name = 'aproperties_es'
    execution_type='testing'
    country='spain'
    locale='es' 
    thousand_separator = ','
    scale_separator = '.'
    custom_settings = {              
        "PROXY_ON" : True,
        "CONCURRENT_REQUESTS": 3,        
        "COOKIES_ENABLED": False,        
        "RETRY_TIMES": 3,        
    }
    download_timeout = 120

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.aproperties.es/search?view=&mod=rental&q=&type%5B%5D=14&type%5B%5D=9&type%5B%5D=10&zone=1&area=0&loc=&group=0&dis=&price-from=&price-to=&surface-from=&surface-to=&order=",
                ],
                "property_type" : "apartment",
                "address" : "Barcelona"
            },
            {
                "url" : [
                    "https://www.aproperties.es/search?view=&mod=rental&q=&type%5B%5D=1&zone=1&area=0&loc=&group=0&dis=&price-from=&price-to=&surface-from=&surface-to=&order=",

                ],
                "property_type" : "house",
                "address" : "Barcelona"
            },
            {
                "url" : [
                    "https://www.aproperties.es/search?view=&mod=rental&q=&type%5B%5D=14&type%5B%5D=9&type%5B%5D=10&zone=2&area=0&loc=&group=0&dis=&price-from=&price-to=&surface-from=&surface-to=&order=",
                ],
                "property_type" : "apartment",
                "address" : "Madrid"
            },
            {
                "url" : [
                    "https://www.aproperties.es/search?view=&mod=rental&q=&type%5B%5D=1&zone=2&area=0&loc=&group=0&dis=&price-from=&price-to=&surface-from=&surface-to=&order=",

                ],
                "property_type" : "house",
                "address" : "Madrid"
            },
                    {
                "url" : [
                    "https://www.aproperties.es/search?view=&mod=rental&q=&type%5B%5D=14&type%5B%5D=9&type%5B%5D=10&zone=3&area=0&loc=&group=0&dis=&price-from=&price-to=&surface-from=&surface-to=&order=",
                ],
                "property_type" : "apartment",
                "address" : "Valencia"
            },
            {
                "url" : [
                    "https://www.aproperties.es/search?view=&mod=rental&q=&type%5B%5D=1&zone=3&area=0&loc=&group=0&dis=&price-from=&price-to=&surface-from=&surface-to=&order=",

                ],
                "property_type" : "house",
                "address" : "Valencia"
            },
            

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),'address': url.get('address')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='propertyBlock__info']/../@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'),'address': response.meta.get('address')})
        
        next_page = response.xpath("//li[@class='pagination__next']/a/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type'),'address': response.meta.get('address')}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", "Aproperties_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title=response.xpath("//div[contains(@class,'_pageTitles__title')]/h1/text()").get()
        if title:
            item_loader.add_value("title", title)
            
        desc = " ".join(response.xpath("//div[@class='content _contentStyle']/p/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        price = response.xpath("//div[@class='propHeader__priceCurrent']//text()").extract_first()
        if price:
            price = price.split("€")[0].replace(".","").strip()
            item_loader.add_value("rent", price)
        item_loader.add_value("currency","EUR")

        item_loader.add_value("address", response.meta.get('address'))
        item_loader.add_value("city", response.meta.get('address'))

        meters = "".join(response.xpath("//div[@class='propHeader__specs__itemLabel'][contains(.,'Superficie')]//following-sibling::div//text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split("m²")[0].strip())
        
        room = response.xpath("//div[contains(.,'Dormitorio')]/following-sibling::div/text()").get() 
        if room:
            item_loader.add_value("room_count",room)
        elif (title and "estudio" in title.lower()) or (desc and "estudio" in desc.lower()):
            item_loader.add_value("room_count", "1")
        else:
            room ="".join(response.xpath("//div[@class='propHeader__specs__itemLabel'][contains(.,'Dormitorios')]//following-sibling::div//text()").extract())
            if room:
                item_loader.add_value("room_count",room)        
        
        bathroom = response.xpath("//div[contains(.,'Baño')]/following-sibling::div/text()").get()
        if bathroom:
             item_loader.add_value("bathroom_count", bathroom.strip())
        # bathroom=response.xpath("//div[@class='propHeader__specs__itemLabel'][contains(.,'Baños')]//following-sibling::div//text()").get()
        # if bathroom:
        #     item_loader.add_value("bathroom_count", bathroom.strip())
            
        latitude_longitude=response.xpath("//script[contains(.,'var latlng')]/text()").get()
        if latitude_longitude:
            latitude=latitude_longitude.split("var latlng")[1].split("LatLng(")[1].split(",")[0].strip()
            longitude=latitude_longitude.split("var latlng")[1].split("LatLng(")[1].split(",")[1].split(")")[0].strip()
            
            item_loader.add_value("latitude",latitude)
            item_loader.add_value("longitude",longitude)
        
        images = [response.urljoin(x) for x in response.xpath("//a[@data-fancybox='gallery']/@href").extract()]
        if images:
            item_loader.add_value("images", images)
        
        furnished = response.xpath("//li[@class='description__featuresCaractListItem']/text()[contains(.,'Amueblado')]").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator=response.xpath("//li[@class='description__featuresCaractListItem']/text()[contains(.,'Ascensor')]").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)

        swimming_pool=response.xpath("//li[@class='description__featuresCaractListItem']/text()[contains(.,'Piscina')]").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        terrace=response.xpath("//li[@class='description__featuresCaractListItem']/text()[contains(.,'Terraza')]").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)

        parking= "".join(response.xpath("//ul/li[contains(.,'parking')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True)

        balcony = "".join(response.xpath("//div[@class='content']//text()[contains(.,'balcón')]").extract())
        if balcony:
            item_loader.add_value("balcony", True)

        item_loader.add_value("landlord_phone", "93 528 89 08")
        item_loader.add_value("landlord_name", "Aproperties Es")
        item_loader.add_value("landlord_email","info@aproperties.es")
       
        yield item_loader.load_item()

        
       

        
        
          

        

      
     