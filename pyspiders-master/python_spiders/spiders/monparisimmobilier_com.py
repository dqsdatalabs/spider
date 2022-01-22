# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'monparisimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):

        
        start_urls = [
            {"url": "https://www.monparisimmobilier.com/louer/?es_search%5Bes_category%5D%5B0%5D=40&es_search%5Bprice%5D%5Bmin%5D&es_search%5Bprice%5D%5Bmax%5D&es_search%5Bbedrooms%5D%5Bmin%5D&es_search%5Bbedrooms%5D%5Bmax%5D"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//ul[contains(@class,'es-listing')]/li"):
            follow_url = response.urljoin(item.xpath(".//a[@class='es-property-link']/@href").get())
            prop_type = item.xpath(".//a[@class='es-property-link']/text()").get()
            if "appartement" in prop_type.lower():
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            elif "maison" in prop_type.lower():
                property_type = "house"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            seen = True
            
        if page == 2 or seen:
            url = f"https://www.monparisimmobilier.com/louer/page/{page}/?es_search%5Bes_category%5D%5B0%5D=40&es_search%5Bprice%5D%5Bmin%5D&es_search%5Bprice%5D%5Bmax%5D&es_search%5Bbedrooms%5D%5Bmin%5D&es_search%5Bbedrooms%5D%5Bmax%5D"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
       
        title = "".join(response.xpath("//h1/a/text()").extract())
        item_loader.add_value("title", title.strip())
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_source", "Monparisimmobilier_PySpider_"+ self.country + "_" + self.locale)
        
        rent=response.xpath("//ul/li[contains(.,'Prix')]/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
        else: return
        
        square_meters=response.xpath("//ul/li[contains(.,'Superficie:')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
        else: return
        
        room_count=response.xpath("//ul/li[contains(.,'Pièces')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else: return
        
        address=response.xpath("//ul/li[contains(.,'Où')]/text()").get()
        if address:
            item_loader.add_value("address", address)
        else: return

        desc="".join(response.xpath("//div[@id='es-description']//p/text()").getall())
        if desc:
            item_loader.add_value("description", desc)
        
        floor=response.xpath("//ul/li[contains(.,'Étage')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
            
        images=[x for x in response.xpath("//div[@class='es-gallery-image']/div/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        utilties=response.xpath("//ul/li[contains(.,'Charges')]/text()").get()
        if utilties:
            item_loader.add_value("utilities", utilties.split('€')[0].strip())
        
        elevator=response.xpath("//ul/li[contains(.,'Ascenseur')]/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
        
        
        yield item_loader.load_item()