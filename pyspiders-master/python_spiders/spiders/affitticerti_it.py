# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest 
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'affitticerti_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Affitticerti_PySpider_italy"

    def start_requests(self):
        start_urls = [ 
            {
                "url": [
                    "https://www.affitticerti.it/property-search/?type%5B%5D=appartamento",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.affitticerti.it/property-search/?type%5B%5D=attico-mansarda&type%5B%5D=casa-indipendente&type%5B%5D=loft-open-space&type%5B%5D=rustico-casale&type%5B%5D=villa&type%5B%5D=villetta-a-schiera"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[article[contains(@class,'property-item')]]"):
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.urljoin(item.xpath(".//h4/a/@href").get()))
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_source", self.external_source)
            dontallow=item.xpath(".//h1[@class='page-title']/span/text()").get()
            if dontallow and "404" in dontallow:
                return 
                 
            title = item.xpath(".//h4//text()").get()
            item_loader.add_value("title", title)
            
            desc = "".join(item.xpath(".//div[@class='detail']//p//text()").getall())
            if desc:
                item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
            
            square_meters = item.xpath(".//span[@class='property-meta-size']/text()").get()
            if square_meters:
                square_meters = square_meters.strip().split(" ")[0]
                item_loader.add_value("square_meters", square_meters)

            
            room_count = item.xpath(".//span[@class='property-meta-bedrooms']/text()").get()
            if room_count:
                room_count = room_count.strip().split("\xa0")[0]
                item_loader.add_value("room_count", room_count)
            
            bathroom_count = item.xpath(".//span[@class='property-meta-bath']/text()").get()
            if bathroom_count:
                bathroom_count = bathroom_count.strip().split(" ")[0]
                item_loader.add_value("bathroom_count", bathroom_count)
            
            rent = "".join(item.xpath(".//h5[@class='price']/text()").getall())
            if rent:
                rent = rent.split(",")[0].replace("€","").strip()
                item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
            
            images = [x for x in response.xpath("//a//img/@src").getall()]
            item_loader.add_value("images", images)
            renta=item_loader.get_output_value("rent")

            url=item_loader.get_output_value("external_link")
            if url:
                yield Request(url, callback=self.extradata, meta={"item_loader":item_loader,"rent":renta})
            
            item_loader.add_value("landlord_name", "AFFITTI CERTI")
            item_loader.add_value("landlord_phone", "3288511828")
            item_loader.add_value("landlord_email", "info@affitticerti.it")

            if page == 2 or seen:
                url = f"https://www.affitticerti.it/property-search/page/{page}/?{response.url.split('/?')[1]}"
                yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})
                
            
    def extradata(self,response):
      
        item_loader=response.meta.get("item_loader")
        rent=response.meta.get("rent")
        adres=response.xpath("//address[@class='title']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
            item_loader.add_value("city","Rome")
            zipcode=re.findall("\d{5}",adres)
            if zipcode:
                item_loader.add_value("zipcode",zipcode)
        bathroom_count=response.xpath("//span[@class='property-meta-bath']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split("\xa0")[0])
        furnished=response.xpath("//div[@class='features']//ul//li[contains(.,'Arredato')]").get()
        if furnished:
            item_loader.add_value("furnished",True)
        balcony=response.xpath("//div[@class='features']//ul//li[contains(.,'Balcone')]").get()
        if balcony:
            item_loader.add_value("balcony",True)
        utilities=response.xpath("//strong[.='Spese di condominio:']/following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("Euro")[0].strip())
        deposit=response.xpath("//strong[.='Deposito cauzionale:']/following-sibling::span/text()").get()
        if deposit:
            if "due" in deposit:
                deposit=2
            item_loader.add_value("deposit",deposit*int(rent))
        latitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("lat")[-1].split(",")[0].replace('":"',"").replace('"',""))
        longitude=response.xpath("//script[contains(.,'lng')]/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("lng")[-1].split(",")[0].replace('":"',"").replace('"',""))

                    
        parking =response.xpath("//div[@class='features']//ul//li[contains(.,'Posto auto')]").get()
        if parking:
            item_loader.add_value("parking", True)
        yield item_loader.load_item() 
        
       