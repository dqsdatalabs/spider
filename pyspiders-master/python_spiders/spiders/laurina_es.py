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

class MySpider(Spider):
    name = 'laurina_es'
    execution_type='testing'
    country='spain'
    locale='es'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.lauria.es/find/?buy_op=rent&kind=flat&town=&zone=&province=&min_bedrooms=0&max_bedrooms=9&min_bathrooms=0&max_bathrooms=9&min_price=0.00&max_price=25000.00&agency=&min_size=&max_size=&sort_by=&page=1",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.lauria.es/find/?buy_op=rent&kind=chalet&town=&zone=&province=&min_bedrooms=0&max_bedrooms=9&min_bathrooms=0&max_bathrooms=9&min_price=0.00&max_price=25000.00&agency=&min_size=&max_size=&sort_by=&page=1",
                    "https://www.lauria.es/find/?buy_op=rent&kind=country_house&town=&zone=&province=&min_bedrooms=0&max_bedrooms=9&min_bathrooms=0&max_bathrooms=9&min_price=0.00&max_price=25000.00&agency=&min_size=&max_size=&sort_by=&page=1",

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

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='house-block-wrapper']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.split("&page")[0] + f"&page={page}"
            yield Request(
                url=url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type'), "page":page+1}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Laurina_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        external_id=response.xpath("//div[@id='property-id']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        title=response.xpath("//h1[@class='property-title']/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        room_count=response.xpath("//ul[@class='amenities']/li[i[contains(@class,'bed')]]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        bathroom_count=response.xpath("//ul[@class='amenities']/li[i[contains(@class,'shower')]]/text()").get()
        if room_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        square_meters=response.xpath("//ul[@class='amenities']/li[i[contains(@class,'square')]]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        rent="".join(response.xpath("//div[@class='price']/span//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        
        address=response.xpath("//h1[@class='property-title']/small/text()").get()
        if address:
            item_loader.add_value("address", address)
            if "," in address:
                try:
                    city = address.split(",")[1].strip()
                    if city:
                        item_loader.add_value("city", city)
                except:
                    pass
        desc="".join(response.xpath("//div[@id='house-description']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc)
            if "balcón" in desc:
                item_loader.add_value("balcony", True)

        
        floor=response.xpath("//ul/li[contains(.,'Planta')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(":")[1].strip())
        
        furnished = response.xpath("//ul/span[contains(.,'amueblado')]/text()").get()
        if furnished:
            if "no amueblado" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
            
        elevator=response.xpath("//ul/span[contains(.,'ascensor')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
            
        terrace=response.xpath("//ul/span[contains(.,'terraza')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        balcony=response.xpath("//ul/span[contains(.,'balcón')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        parking=response.xpath("//ul/span[contains(.,'garaje')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        swimming_pool=response.xpath("//ul/span[contains(.,'piscina')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        images=[x for x in response.xpath("//div[@id='property-detail-large']/div/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
       
        latitude_longitude=response.xpath("//script[contains(.,'L.map')]/text()").get()
        if latitude_longitude:
            lat=latitude_longitude.split("coords = [")[1].split(",")[0]
            lng=latitude_longitude.split("coords = [")[1].split(",")[1].split("]")
            if lat or lng:
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)
       
        item_loader.add_value("landlord_name","LAURIA INMOBILIARIA")
        item_loader.add_value("landlord_phone","963509287")
        item_loader.add_value("landlord_email","lauria@lauria.es")
       
        yield item_loader.load_item()

        
       

        
        
          

        

      
     