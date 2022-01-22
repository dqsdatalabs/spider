# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'berlinovo_de'
    external_source = "Berlinovo_PySpider_germany_de"
    execution_type='testing'
    country='germany'
    locale='de'    

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.berlinovo.de/de/suche-wohnungen?",
                ],
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//span[@class ='field-content']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.berlinovo.de/de/suche-wohnungen?address=&field_location_geofield_latlon[radius]=5&field_location_geofield_latlon[lat]=&field_location_geofield_latlon[lng]=&page={page-1}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        property_type= response.xpath("//span[contains(.,'Kategorie:')]//following-sibling::span//text()").get()
        if property_type and "Etagenwohnung" in property_type.lower():
            item_loader.add_value("property_type","house")
        else: 
            item_loader.add_value("property_type","apartment")

        external_id= response.xpath("//span[contains(.,'Objektnummer:')]//following-sibling::span//text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
 
        title= response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)  
                 
        address= response.xpath("//span[contains(@class,'address')]//text()").get()
        if address:
            item_loader.add_value("address",address) 

        zipcode= response.xpath("//span[contains(@class,'address')]//text()").get()
        if zipcode:
            zipcode = zipcode.split(" ")[0]
            item_loader.add_value("zipcode",zipcode)

        city= response.xpath("//span[contains(@class,'address')]//text()").get()
        if city:
            city = city.split(" ")[1]
            item_loader.add_value("city",city)

        description= response.xpath("//div[contains(@class,'field-item even')]//p//text()").getall()
        if description:
            item_loader.add_value("description",description)

        rent= response.xpath("//span[contains(.,'Warmmiete:')]//following-sibling::span//text()").get()
        if rent:
            rent=rent.split("€")[0]
            if rent and "," in rent:
                rent=rent.split(",")[0]
            item_loader.add_value("rent",rent) 
        item_loader.add_value("currency",'EUR')

        deposit= response.xpath("//span[contains(.,'Kautionsbetrag:')]//following-sibling::span//text()").get()
        if deposit:
            deposit=deposit.split("€")[0]
            if deposit and "," in deposit:
                deposit=deposit.split(",")[0]
            item_loader.add_value("deposit",deposit) 

        square_meters= response.xpath("//span[contains(.,'Wohnfläche:')]//following-sibling::span//text()").get()
        if square_meters:
            square_meters=square_meters.split("m²")[0]
            item_loader.add_value("square_meters",square_meters) 

        room_count = response.xpath("//span[contains(.,'Zimmer:')]//following-sibling::span//text()").get()
        if room_count:
            if room_count and "," in room_count:
                room_count = room_count.split(",")[0]
            item_loader.add_value("room_count",room_count) 

        floor= response.xpath("//span[contains(.,'Etage:')]//following-sibling::span//text()").get()
        if floor:
            item_loader.add_value("floor",floor) 
            
        available_date= response.xpath("//span[contains(.,'Verfügbar ab:')]//following-sibling::span//text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)

        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'ad-thumb-list')]//li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Berlinovo Immobilien")
        item_loader.add_value("landlord_phone", "+49 30 25441-0")
        item_loader.add_value("landlord_email", "welcome@berlinovo.de")

        yield item_loader.load_item()