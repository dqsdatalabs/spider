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
    name = 'konutrealty_com'
    execution_type='testing'
    country='turkey'
    locale='tr'
    external_source='Konutrealty_PySpider_turkey_tr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://konutrealty.com/ilanlar/kiralik?page=1",
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                        )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False

        for item in response.xpath("//div[@class='cs-tr']//div[@class='cs-td']//img//parent::a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen=True
        
        if page == 2 or seen:
            url = response.url.replace(f"page={page-1}", f"page={page}")
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)

        property_type= response.xpath("//h3//text()").get()
        if property_type and "daire" in property_type.lower():
            property_type = "house"
        elif property_type and "apartman" in property_type.lower():
            property_type = "apartment"
        else:
            return
        item_loader.add_value("property_type", property_type)

        if property_type:

            title = response.xpath("//h3//text()").get()
            if title:
                item_loader.add_value("title", title)

            external_id = response.xpath("//td[contains(.,'İlan No')]//following-sibling::td//text()").get()
            if external_id:
                item_loader.add_value("external_id", external_id)
            
            address="".join(response.xpath("//td[contains(.,'Semt')]//following-sibling::td//text()").get())
            if address:
                item_loader.add_value("address", address.strip())

            city = response.xpath("//td[contains(.,'Semt')]//following-sibling::td//text()").get()
            if city: 
                item_loader.add_value("city", city.strip())
            
            room_count="".join(response.xpath("//td[contains(.,'Oda Sayısı')]//following-sibling::td//text()").get())
            if room_count:
                room_count=room_count.split("+")
                item_loader.add_value("room_count", str(int(room_count[0])+int(room_count[1])))
            
            bathroom_count="".join(response.xpath("//td[contains(.,'Banyo Sayısı')]//following-sibling::td//text()").get())
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)
            
            square_meters="".join(response.xpath("//td[contains(.,'brüt/net')]//following-sibling::td//text()").get())
            if square_meters:
                square_meters = square_meters.split("/")
                square_meters = [int(x.split("m")[0].strip()) for x in square_meters]
                square_meters = min(square_meters)
                item_loader.add_value("square_meters",str(square_meters))
            
            rent="".join(response.xpath("//td[contains(.,'Fiyat')]//following-sibling::td//text()").get())
            if rent:
                rent = rent.split("₺")[1]
                item_loader.add_value("rent", rent.strip().replace("\xa0",".").replace(",",""))
                item_loader.add_value("currency", "TRY")


            utilities="".join(response.xpath("//td[contains(.,'Aidat')]//following-sibling::td//text()").extract())
            if utilities and "₺" in utilities:
                utilities=utilities.split("₺")[0].strip()
                item_loader.add_value("utilities", utilities)
            
            floor="".join(response.xpath("//td[contains(.,'Katı')]//following-sibling::td//text()").get())
            if floor:
                item_loader.add_value("floor", floor.strip())
            
            furnished="".join(response.xpath("//td[contains(.,'Eşyalı')]//following-sibling::td//text()").get())
            if furnished and "hayır" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
                
            elevator=response.xpath("//div[@class='d-flex flex-wrap justify-content-between']//ul//li[contains(.,'Asansör')]//text()").get()
            if elevator:
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)     

            terrace=response.xpath("//div[@class='d-flex flex-wrap justify-content-between']//ul//li[contains(.,'Teras')]//text()").get()
            if terrace:
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)  
                            
            balcony=response.xpath("//div[@class='d-flex flex-wrap justify-content-between']//ul//li[contains(.,'Balkon')]//text()").get()
            if balcony:
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)  

            washing_machine=response.xpath("//div[@class='d-flex flex-wrap justify-content-between']//ul//li[contains(.,'Beyaz Eşya')]//text()").get()
            if washing_machine:
                item_loader.add_value("washing_machine", True)
            else:
                item_loader.add_value("washing_machine", False)  

            parking=response.xpath("//div[@class='d-flex flex-wrap justify-content-between']//ul//li[contains(.,'Otopark')]//text()").get()
            if parking:
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)  

            desc="".join(response.xpath("//li[contains(@class,'active')]//p[@style='text-align: center;']//text()").getall())
            if desc:
                item_loader.add_value("description", desc.strip())

            images = [response.urljoin(x)for x in response.xpath("//a[contains(@data-fancybox,'products')]//@href").extract()]
            if images:
                    item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

            item_loader.add_value("landlord_name","Konut Realty")
            item_loader.add_value("landlord_phone", "+90 212 251 87 87")
            item_loader.add_value("landlord_email", "info@konutrealty.com")

            yield item_loader.load_item()