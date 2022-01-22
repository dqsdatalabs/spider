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
    name = 'hzinmobiliarias_com'
    execution_type='testing'
    country='spain'
    locale='es'
    external_source="Hzinmobiliarias_PySpider_spain"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://hzinmobiliarias.com/busqueda/alquilar",
                ],
                "property_type" : "apartment",
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False

        for item in response.xpath("//div[@class='real-estate-item']//h3//a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        if page == 2 or seen:
            url =f"https://hzinmobiliarias.com/busqueda/alquilar?page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title","//title//text()")

        rent=response.xpath("//p[@class='precio']/text()").get()
        if rent:
            rent=rent.split("EUR")[0]
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        external_id=response.xpath("//p[@class='referencia']/text()").get()
        if external_id:
            external_id=external_id.split(":")[-1]
            item_loader.add_value("external_id",external_id)
        adres="".join(response.xpath("//div[@class='direccion']//p//text()").getall())
        if adres:
            item_loader.add_value("address",re.sub('\s{2,}', ' ', adres.strip()))
        zipcode=item_loader.get_output_value("address")
        if zipcode:
            zipcode=re.search("\d{5}",zipcode)[0]
            item_loader.add_value("zipcode",zipcode)
        desc="".join(response.xpath("//div[@class='descripcion']//div/text()").getall())
        if desc:
            item_loader.add_value("description",desc) 
            if "comercial" in desc.lower():
                return
            if "OFICINA" in desc.upper():
                return
            if "local" in desc.lower():
                return
            if "RESTAURANT" in desc.upper():
                return
        
        garage_test = response.xpath("//span[contains(text(),'GARAJE')]").get()
        if garage_test:
            return

        latitude=response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude:
            latitude=latitude.split("L.LatLng")[-1].split(",")[0].replace("(","")
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if longitude:
            longitude=longitude.split("L.LatLng")[-1].split(",")[1].replace(")","").strip()
            item_loader.add_value("longitude",longitude)
        images=[x for x in response.xpath("//div[@id='imagenes']//img/@data-lazy").getall()]
        if images:
            item_loader.add_value("images",images)
      
        square_meters=response.xpath("//i[@class='lni lni-grid-alt']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.strip())

        bathroom_count=response.xpath("//i[@class='las la-bath']/following-sibling::text()").get() 
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        elevator=response.xpath("//li[contains(.,'Ascensor:')]/text()").get()
        if elevator and "sí" in elevator.lower():
            item_loader.add_value("elevator",True)

        room_count=response.xpath("//i[@class='las la-bed']/following-sibling::text()").get() 
        if room_count:
            item_loader.add_value("room_count",room_count.strip())

        city = response.xpath("//div[@class='titulo']/h4/text()[last()]").get()
        if city:
            item_loader.add_value("city",city.strip())


        item_loader.add_xpath("landlord_name","//p[@class='nombreAgente']/text()")
        item_loader.add_xpath("landlord_email","//a[@class='email']/text()")
        item_loader.add_value("landlord_phone","+34 928 24 24 24")

        yield item_loader.load_item()