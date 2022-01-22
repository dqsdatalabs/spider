# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request 
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from scrapy import Request,FormRequest
import json

class MySpider(Spider):
    name = "immobrussels_be" 
    execution_type = 'testing'
    country = 'belgium'
    locale='fr'
    external_source='immobrussels_PySpider_belgium'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.immobrussels.be/fr/immo-a-louer/appartement/auderghem-1160/",
                "property_type" : "apartment",
            },
            {
                "url" : "https://www.immobrussels.be/fr/immo-a-louer/maison/auderghem-1160/",
                "property_type" : "house",
            },
        ] # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse, meta={'property_type': url.get('property_type'),'type': url.get('type'),'property_type1': url.get('property_type1')})
    def parse(self, response):
        page = response.meta.get("page", 1)
        seen = False
        if "ajax" in response.url:
            data=json.loads(response.body)['ads']
            list=Selector(text=data).xpath("//a[contains(@class,'stretched-link')]/@href").getall()
            for item in list:
                follow_url = response.urljoin(item)
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
                seen = True
        else:
            for item in response.xpath("//a[contains(@class,'stretched-link')]/@href").getall():
                follow_url = response.urljoin(item)
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
                seen = True

        if page == 1 or seen:
            next_page = f"https://www.immobrussels.be/wp-admin/admin-ajax.php?action=dp_ajax&dp_ajax_data%5Bcontext%5D%5Blang%5D=fr&dp_ajax_data%5Bcontext%5D%5Bis_bilingual%5D=true&dp_ajax_data%5Bcontext%5D%5Brequest%5D%5Bpath%5D=%2Ffr%2Fimmo-a-louer%2Fappartement%2Fauderghem-1160%2F&dp_ajax_data%5Bcontext%5D%5Brequest%5D%5Bhas_lang_path%5D=1&dp_ajax_data%5Bcontext%5D%5Brequest%5D%5Bproperty_type%5D=appartement&dp_ajax_data%5Bcontext%5D%5Brequest%5D%5Bgeo_zone%5D%5Blocalite%5D=auderghem&dp_ajax_data%5Bcontext%5D%5Brequest%5D%5Bgeo_zone%5D%5Bzip%5D=1160&dp_ajax_data%5Bcontext%5D%5Brequest%5D%5Bparameters%5D%5Bpage%5D={page}&dp_ajax_data%5Bmethod%5D=get_ads&dp_ajax_uuid="
            if next_page:        
                yield Request(next_page,callback=self.parse,meta={'property_type': response.meta.get('property_type'),'page':page+1})
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type",response.meta.get('property_type'))
        dontallow=response.url
        if dontallow and "logic-immo" in dontallow:
            return 

        rent=response.xpath("//span[@class='d-block price-label']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace(" ","").replace("\u202f",""))
        item_loader.add_value("currency","EUR")
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//span[@class='city-line pl-1']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
            zipcode=adres.split(" ")[0]
            if zipcode:
                item_loader.add_value("zipcode",zipcode)
            city=adres.split(" ")[1]
            if city:
                item_loader.add_value("city",city)
        room_count=response.xpath("//div[contains(@class,'NrOfBedrooms')]//div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//div[contains(@class,'NrOfBathrooms')]//div[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        square_meters=response.xpath("//div[contains(@class,'LivableSurface')]//div[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        description=response.xpath("//div[@class='dynamic-description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div[@class='carousel-item']//img//@data-src").getall()]
        if images:
            item_loader.add_value("images",images)

        yield item_loader.load_item()
