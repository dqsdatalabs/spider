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

class MySpider(Spider):
    name = 'samyap_com'
    execution_type='testing'
    country='turkey'
    locale='tr'   
    def start_requests(self):
        start_urls = [
            {
                "url" : "http://samyap.com.tr/tr/Kategoriler/Kiral%C4%B1k-Daire",
                "property_type" : "apartment"
            }
        ]
        for url in start_urls:
            yield Request(url=url.get('url'), callback=self.parse,
                                    meta={'property_type': url.get('property_type')}) 



    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='col-sm-3']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Samyap_PySpider_"+ self.country + "_" + self.locale)
 
        title = response.xpath("//h3[@class='panel-title']/i[3]/text()").get()
        item_loader.add_value("title", title.replace("\\", "\'"))
        item_loader.add_value("external_link", response.url)
        external_id="".join(response.xpath("//h3[@class='panel-title']/text()").extract())
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[1].strip())
        
       
        item_loader.add_value("property_type", response.meta.get("property_type"))
        
        address = response.xpath("//div[@class='panel-body']/div[1]/i/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split('/')[0].strip())

        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(",")[0].strip()
            longitude = latitude_longitude.split('lng:')[1].split("}")[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            
        square_meters=response.xpath("//div[@class='col-sm-3']/table//tr/td[contains(.,'Net Kullanım Alanı')]/following-sibling::td/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        rent=response.xpath("//h3[@class='panel-title']/i[2]/text()").extract_first()
        if rent:
            item_loader.add_value("rent", rent.split(" ")[0])
            item_loader.add_value("currency", "TRY")
        
        
        room_count=title.split(" ")
        for i in room_count:
            if "+" in i:      
                i=i.split("+")          
                item_loader.add_value("room_count",str(int(i[0])+int(i[1])))
                break
            
        floor=response.xpath("//div[@class='col-sm-3']/table//tr/td[contains(.,'Kaçıncı Kat')]/following-sibling::td/text()").extract_first()
        if floor:
            item_loader.add_value("floor", floor)
            
        bathroom=response.xpath("//div[@class='col-sm-3']/table//tr/td[contains(.,'Banyo')]/following-sibling::td/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
            
        utilities=response.xpath("//div[@class='col-sm-3']/table//tr/td[contains(.,'Aidat')]/following-sibling::td/text()[.!='0']").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities)
            
        desc="".join(response.xpath("//div[@class='col-sm-12']/*[self::p or self::ul/li]//text()").extract())
        if desc:
            item_loader.add_value("description", desc)
        
        images=[x for x in response.xpath("//div[@class='panel-body']/div/a/img/@src").getall()]
        for i in images:
            item_loader.add_value("images", "http://samyap.com.tr"+i)
        
        
        name=response.xpath("//div[@class='tum-kenar']/i/text()").extract_first()
        if name:
            item_loader.add_value("landlord_name", name.strip())
        phone="".join(response.xpath("//div[@class='tum-kenar']/p/text()").extract())
        if phone:
            item_loader.add_value("landlord_phone", phone.split("Broker")[1].strip())
        email=response.xpath("//div[@class='tum-kenar']/p/a/text()").extract_first()
        if email:
            item_loader.add_value("landlord_email", email.strip())
            
            
        elevator=response.xpath("//table[@border='1']/tr/td/i/text()[contains(.,'Asansör')]").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)
        else:
            elevator = response.xpath("//table//td/del[contains(.,'Asansör')]/text()").extract_first()
            if elevator:
                item_loader.add_value("elevator", False)
                
        furnished=response.xpath("//table[@border='1']/tr/td/i/text()[contains(.,'Eşyalı')]").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            furnished = response.xpath("//table//td/del[contains(.,'Eşyalı')]/text()").extract_first()
            if furnished:
                item_loader.add_value("furnished", False)
        
        balcony=response.xpath("//table[@border='1']/tr/td/i/text()[contains(.,'Balkon')]").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)

        parking=response.xpath("//table[@border='1']//tr/td/i//text()[contains(.,'Otopark')]").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking=response.xpath("//table//td/del[contains(.,'Otopark')]/text()").extract_first()
            if parking:
                item_loader.add_value("parking", False)

        swimming_pool=response.xpath("//table[@border='1']//tr/td/i//text()[contains(.,'Havuz')]").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        else:
            swimming_pool = response.xpath("//table//td/del[contains(.,'Havuz')]/text()").extract_first()
            if swimming_pool:
                item_loader.add_value("swimming_pool", False)
        yield item_loader.load_item()