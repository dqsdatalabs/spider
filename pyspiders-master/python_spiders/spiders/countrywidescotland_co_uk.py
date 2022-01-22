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
import re
import scrapy

class MySpider(Spider):
    name = 'countrywidescotland_co_uk'
    execution_type='testing' 
    country='united_kingdom'
    locale='en'
    external_source="Countrywidescotland_PySpider_united_kingdom_en"

    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.countrywidescotland.co.uk/search.ljson?channel=lettings&fragment=tag-flat/most-recent-first/status-all/page-1",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.countrywidescotland.co.uk/search.ljson?channel=lettings&fragment=tag-house/most-recent-first/status-all/page-1",
                "property_type" : "house"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        data=json.loads(response.body)['properties']
        for item in data:
            bathrooms=item['bathrooms']
            item=f"https://www.countrywidescotland.co.uk/"+item['url']
            yield Request(item, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'),"bathrooms":bathrooms})
            seen = True
        if page == 2 or seen:
            a=f"page-{page}"
            url = str(response.url).replace("page-1",a)
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item=response.meta.get('item')
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        address ="".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if address:
            item_loader.add_value("address", address.split("-property-address:")[1].split("-->")[0].replace('"',""))
        zipcode ="".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split("property-postcode:")[1].split("-->")[0].replace('"',""))
        rent = "".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if rent:
            item_loader.add_value("rent", rent.split("property-price:")[1].split("-->")[0].replace('"',""))
        item_loader.add_value("currency", "GBP")
        
        room_count = "".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        item_loader.add_value("room_count", room_count.split('property-bedrooms:"')[1].split("-->")[0].replace('"',""))
        
        bathroom_count =response.meta.get('bathrooms')
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        description = "".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if description:
            item_loader.add_value("description",description.split('--property-description:"')[1].split("-->")[0].replace('"',""))
            
        images= "".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if images:
            images = [x.split('"-->')[0]for x in images.split('--property-images:"')]
            if images:
                item_loader.add_value("images", images)
                
        latitude=  "".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if latitude:
            item_loader.add_value("latitude", latitude.split('property-latitude:"')[1].split('"-->')[0])   
                       
        longitude=  "".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if longitude:
            item_loader.add_value("longitude", longitude.split('--property-longitude:"')[1].split('"-->')[0])     
        
        available_date="".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if available_date:
            item_loader.add_value("available_date",available_date.split("property-live-date:")[1].split("-->")[0].replace('"',""))
        
        name="".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if name:
            item_loader.add_value("landlord_name",name.split('--property-office-name:"')[1].split("-->")[0].replace('"',""))
                
        email="".join(response.xpath("//section[@class='mini-contact']/following-sibling::comment()").extract())
        if email:
            item_loader.add_value("landlord_email",email.split('--property-email:"')[1].split("-->")[0].replace('"',""))
        
        yield item_loader.load_item()

