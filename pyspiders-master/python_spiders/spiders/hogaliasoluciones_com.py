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
    name = 'hogaliasoluciones_com'
    execution_type='testing'
    country='spain'
    locale='es'
   
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.hogaliasoluciones.com/find/?buy_op=rent&kind=flat&town=&zone=&province=&min_bedrooms=0&max_bedrooms=9&min_bathrooms=0&max_bathrooms=9&min_price=&max_price=&agency=&min_size=&max_size=&sort_by=&page=1",
                    "https://www.hogaliasoluciones.com/find/?buy_op=rent&kind=shop&town=&zone=&province=&min_bedrooms=0&max_bedrooms=9&min_bathrooms=0&max_bathrooms=9&min_price=&max_price=&agency=&min_size=&max_size=&sort_by=&page=1",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.hogaliasoluciones.com/find/?buy_op=rent&kind=chalet&town=&zone=&province=&min_bedrooms=0&max_bedrooms=9&min_bathrooms=0&max_bathrooms=9&min_price=&max_price=&agency=&min_size=&max_size=&sort_by=&page=1",

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
        item_loader.add_value("external_source", "Hogaliasoluciones_PySpider_"+ self.country + "_" + self.locale)

        local = "".join(response.xpath("//ul[@class='amenities']/li[.=' Local']//text()").extract()).strip()
        if local !='Local':
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_link", response.url)

            external_id=response.xpath("//div[@id='property-id']/text()").get()
            if external_id:
                item_loader.add_value("external_id", external_id.split(":")[1].strip())

            title=response.xpath("//h1[@class='property-title']/text()").get()
            if title:
                item_loader.add_value("title", title.strip())


            bathroom=response.xpath("//ul[@class='amenities']/li/i[@class='fas fa-shower']/following-sibling::text()").get()
            if bathroom:
                item_loader.add_value("bathroom_count", bathroom.strip())
            
            room_count=response.xpath("//ul[@class='amenities']/li[contains(.,'m2')]//following-sibling::li[1]/text()").get()
            if room_count:
                if room_count.strip() != '0':
                    item_loader.add_value("room_count", room_count.strip())
            
            square_meters=response.xpath("//ul[@class='amenities']/li[contains(.,'m2')]/text()").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
            
            rent="".join(response.xpath("//div[@class='price']/span//text()").getall())
            if rent:
                item_loader.add_value("rent_string", rent)
            
            address=response.xpath("//h1[@class='property-title']/small/text()").get()
            if address:
                item_loader.add_value("address", address)
                if "," in address:
                    item_loader.add_value("city", address.split(",")[1])
                else:
                    item_loader.add_value("city", address)
                    
            desc="".join(response.xpath("//div[@id='house-description']/p/text()").getall())
            if desc:
                item_loader.add_value("description", desc)
            
            floor=response.xpath("//ul/li[contains(.,'Planta')]/text()").get()
            if floor:
                item_loader.add_value("floor", floor.split(":")[1].strip())

            balcony="".join(response.xpath("//span[contains(@class,'label')][contains(.,'balcón')]").getall())
            if balcony:
                item_loader.add_value("balcony", True)

            piscina="".join(response.xpath("//span[contains(@class,'label')][contains(.,'piscina')]/text()").getall())
            if piscina:
                item_loader.add_value("swimming_pool", True)
            
            furnished=response.xpath("//ul/span[contains(.,'amueblado')]/text()").get()
            if furnished:
                item_loader.add_value("furnished", True)

            furnished_false=response.xpath("//ul/span[contains(.,'SIN AMUEBLAR')]/text()").get()
            if furnished_false:
                item_loader.add_value("furnished", False)


            elevator=response.xpath("//ul/span[contains(.,'ascensor')]/text()").get()
            if elevator:
                item_loader.add_value("elevator", True)
                
            terrace=response.xpath("//ul/span[contains(.,'terraza')]/text()").get()
            if terrace:
                item_loader.add_value("terrace", True)
            
            images=[x for x in response.xpath("//div[@id='property-detail-large']/div/a/img/@src").getall()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", str(len(images)))
        
            latitude_longitude=response.xpath("//script[contains(.,'L.map')]/text()").get()
            if latitude_longitude:
                lat=latitude_longitude.split("coords = [")[1].split(",")[0].strip()
                lng=latitude_longitude.split("coords = [")[1].split(",")[1].split("]")[0].strip()
                if lat or lng:
                    item_loader.add_value("latitude", lat)
                    item_loader.add_value("longitude", lng)
            
            energy="".join(response.xpath("//ul[contains(@class,'property-features')]/li[contains(.,'energético')]//text()").getall())
            if "(" in energy:
                label = energy.strip().split(":")[1].strip().split(" ")[0]
                item_loader.add_value("energy_label", label)
            
            item_loader.add_value("landlord_name","HOGALIA SEVILLA")
            item_loader.add_value("landlord_phone","954318883")
            item_loader.add_value("landlord_email","info.sevilla@hogaliasoluciones.com")
        
            yield item_loader.load_item()

        
       

        
        
          

        

      
     