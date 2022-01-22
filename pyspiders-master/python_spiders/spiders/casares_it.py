# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'casares_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Casares_PySpider_italy"
    url = "https://casares.it/search-results/?keyword=&location%5B%5D=&status%5B%5D=affitto&bedrooms=&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=&label%5B%5D="
    
    def start_requests(self):
    
        yield Request(
            url=self.url, 
            callback=self.parse,
        )
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//a[@class='btn btn-primary btn-item ']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
                
        if page == 2 or seen:
            url = f"https://casares.it/search-results/page/{page}/?keyword&location%5B0%5D&status%5B0%5D=affitto&bedrooms&bathrooms&min-area&max-area&min-price&max-price&property_id&label%5B0%5D"
            yield Request(url, callback=self.parse, meta={"page": page+1})
        
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)
        external_id=response.xpath("//strong[.='codice immobile:']/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        title=response.xpath("//div[@class='page-title']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=" ".join(response.xpath("//ol[@class='breadcrumb']//li[2 or 3]/a/span/text()").getall())
        if property_type:
            if "Appartamento" in property_type:
                item_loader.add_value("property_type","apartment")
            if "Attico" in property_type:
                item_loader.add_value("property_type","apartment")
            if "Camera" in property_type:
                item_loader.add_value("property_type","apartment")
        typecheck=item_loader.get_output_value("property_type")
        if not typecheck:
            return 
        rent=response.xpath("//li[@class='item-price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(",")[0].replace("€",""))
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//address[@class='item-address']/i/following-sibling::text()").get()
        if adres and len(adres)>5:
            item_loader.add_value("address",adres)
        city=response.xpath("//address[@class='item-address']/i/following-sibling::text()").get()
        if city:
            item_loader.add_value("city",city.split(",")[-1])
        zipcode=response.xpath("//address[@class='item-address']/i/following-sibling::text()").get()
        if zipcode:
            zipcode=re.findall("\d{5}",zipcode)
            if zipcode:
                item_loader.add_value("zipcode",zipcode)
        desc=" ".join(response.xpath("//h2[.='descrizione']/parent::div/following-sibling::div//p//text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        furnished=response.xpath("//h2[.='caratteristiche']/parent::div/following-sibling::div/ul/li/a/text()").get()
        if furnished and "Arredato"==furnished:
            item_loader.add_value("furnished",True)

        balcony=response.xpath("//li//a[contains(.,'balcone')]/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        parking=response.xpath("//li//a[contains(.,'posto auto')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        washing_machine=response.xpath("//li//a[contains(.,'Lavatrice')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine",True)

        images=[x for x in response.xpath("//div[@class='top-gallery-section']//img//@src").getall()]
        for i in images:
            if "casares" in i:
                item_loader.add_value("images",i)
        latitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude:
            latitude=latitude.split('lat"')[1].split(",")[0].replace('"',"").replace(':',"")
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if longitude:
            longitude=longitude.split('lng"')[1].split(",")[0].replace('"',"").replace(':',"")
            item_loader.add_value("longitude",longitude)
        room=response.xpath("//strong[.='stanze da letto:']/following-sibling::span/text() | //strong[.='stanza da letto:']/following-sibling::span/text()").get()
        if room:
            item_loader.add_value("room_count",room)
        bathroom=response.xpath("//strong[.='stanze da bagno:']/following-sibling::span/text() | //strong[.='stanza da bagno:']/following-sibling::span/text()").get()
        if room:
            item_loader.add_value("bathroom_count",bathroom)
        square_meters=response.xpath("//strong[contains(.,'dimensioni dell')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        phone=" ".join(response.xpath("//h2[.='descrizione']/parent::div/following-sibling::div//p//text()").getall())
        if phone:
            phone=phone.split("Tel")[-1].split(" –")[0]
            if phone:  
                item_loader.add_value("landlord_phone",phone.replace(".",""))
        item_loader.add_value("landlord_name","GABETTI")
        item_loader.add_value("landlord_email"," info@casares.it")
            
        

                
        yield item_loader.load_item()
