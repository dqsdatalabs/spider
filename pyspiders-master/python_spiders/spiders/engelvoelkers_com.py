# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from  geopy.geocoders import Nominatim
import re

class MySpider(Spider):
    name = 'engelvoelkers_com'
    execution_type='testing'
    country='france' 
    locale='fr'
    external_source='Engelvoelkers_PySpider_france_fr'
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.engelvoelkers.com/fr/search/?q=&startIndex=0&businessArea=residential&sortOrder=DESC&sortField=sortPrice&pageSize=18&facets=cntry%3Afrance%3Bbsnssr%3Aresidential%3Btyp%3Arent%3B",
            },

            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse
                            )


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='row ev-search-results']/div/a/@href").extract():
            print("---------",item)
            f_url = response.urljoin(item)
            yield Request( 
                f_url, 
                callback=self.populate_item
              
            )
        
        next_page = response.xpath("//a[@class='ev-pager-next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Engelvoelkers_PySpider_"+ self.country + "_" + self.locale)

        property_type=response.xpath("//div[@class='ev-exposee-detail']/div/text()").get()
        if property_type:
            if "appartement" in property_type.lower():
                item_loader.add_value("property_type","apartment")
            if "maison" in property_type.lower():
                item_loader.add_value("property_type","house")
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        external_id = response.xpath("//label[contains(.,'E&V ID')]/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        address = response.xpath("substring-after(//div[contains(@class,'ev-exposee-subtitle')]/text(),'|')").get()
        if address:
            item_loader.add_value("address",address.strip())
            item_loader.add_value("city", address.split(",")[-2].strip())

        rent=response.xpath("substring-after(//ul/li[contains(.,'Loyer')]/span/text(),'Loyer ')").get()
        if rent and rent != "0":
            item_loader.add_value("rent", rent.replace(",","").replace("EUR","").strip())
        item_loader.add_value("currency", "EUR")
        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            rent=response.xpath("//img[@title='Prix-global']/following-sibling::div/text()").get()
            if rent:
                item_loader.add_value("rent",rent.split("E")[0].replace(".","").strip())

        square_meters=response.xpath("//div[contains(@class,'ev-key-fact')]/div[contains(.,'Wohnfläche') or contains(.,'Surface')]/div[contains(@class,'value')]/text()").get()
        if square_meters:
            square_mt=square_meters.split("m²")[0].replace(",","").split(".")[0].strip()
            if square_mt=="0":
                pass
            else:
                item_loader.add_value("square_meters",square_mt)
        
        room_count=response.xpath("//div[contains(@class,'ev-key-fact')]/div[contains(.,'Zimmer') or contains(.,'Chambres') or contains(.,'Pièce')]/div[contains(@class,'value')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count=response.xpath("//div[contains(@class,'ev-key-fact')]/div[contains(.,'Zimmer') or contains(.,'bains') or contains(.,'Pièce')]/div[contains(@class,'value')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        parking=response.xpath("//label[.='Parkings']/text() | //label[.='Parking']/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        
        desc="".join(response.xpath("//ul[contains(@class,'ev-exposee-content')]//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

            
        images=[x for x in response.xpath("//div[contains(@class,'ev-image-gallery')]/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        name=response.xpath("//span[@itemprop='name']//text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        phone=response.xpath("//span[@itemprop='telephone']//text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("+",""))

        yield item_loader.load_item()

