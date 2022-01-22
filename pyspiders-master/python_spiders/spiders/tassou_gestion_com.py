# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import itemadapter
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'tassou_gestion_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Tassou_Gestion_PySpider_france"
    custom_settings = {
        "HTTPCACHE_ENABLED":False, 
    }

    headers = {'content-type': "application/json"}   
    payload = '{"filters":[{"field":"BusinessTypeID","value":"2","type":0},{"field":"OfficeNumber","value":"74982","type":0},{"field":"ListingClassID","value":"1","type":0}],"sort":{"fieldToSort":"ContractDate","order":1}}'
    def start_requests(self):
        url = "https://www.remax.fr/Api/Listing/MultiMatchSearch?page=0&searchValue=&size=20"
        yield Request(
            url,
            headers = self.headers,
            body = self.payload,
            method = "POST",
            callback = self.parse
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 1)
        seen = False
        data=json.loads(response.body)['results']

        for item in data:
            latitude=item['latitude']
            longitude=item['longitude']
            price=item['listingPrice']
            follow_url = f"https://s.maxwork.fr/site/static/2/listings/searchdetails_V2/{item['listingTitle']}.html"
            yield Request(follow_url, callback=self.jump,meta={'latitude':latitude,'longitude':longitude,'price':price})
            seen = True
        if page == 1 or seen:
            payload='{"filters":[{"field":"BusinessTypeID","value":"2","type":0},{"field":"ListingClassID","value":"1","type":0},{"field":"ContractDate","greaterThan":"2021-11-01T21:00:00.000Z","lessThan":"2021-12-02T11:09:55.555Z","type":3}],"sort":{"fieldToSort":"ContractDate","order":1}}'
            url = "https://www.remax.fr/Api/Listing/MultiMatchSearch?page={page}&searchValue=&size=20"
            yield Request(
                url,
                headers = self.headers,
                body = payload,
                method = "POST",
                callback = self.parse,
                meta={"page": page+1}
                )
    def jump(self,response):
        latitude=response.meta.get('latitude')
        longitude=response.meta.get('longitude')
        rent=response.meta.get('price')

        type=response.xpath("//li[@class='listing-type']/text()").get()
        if type:type=type.lower()
        bedroom=response.xpath("//li[@class='listing-bedroom']/i/following-sibling::text()").get()
        if bedroom:bedroom=bedroom.strip()
        bathroom=response.xpath("//li[@class='listing-bathroom']/i/following-sibling::text()").get()
        if bathroom:bathroom=bathroom.strip()
        adres=response.xpath("//h2[@class='listing-address']/span/text()").get()
        if adres:adres1=adres.strip().replace(", ","-").replace(" - ","---").lower().strip()
        id=response.url.split("/")[-1].split(".html")[0]
        url=f"https://www.remax.fr/mandats/location-{type}-t{bedroom}-{adres1}/{id}"
        yield Request(url,callback = self.populate_item,meta={'latitude':latitude,'longitude':longitude,'rent':rent,'type':type,'adres':adres,'bathroom':bathroom,'bedroom':bedroom})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link",response.url)
        external_id=response.url.split("/")[-1]
        if external_id:
            item_loader.add_value("external_id",external_id)
        latitude=response.meta.get('latitude')
        if latitude:
            item_loader.add_value("latitude",str(latitude))
        longitude=response.meta.get('longitude')
        if longitude:
            item_loader.add_value("longitude",str(longitude))
        rent=response.meta.get('rent')
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        property_type=response.meta.get('type')
        if property_type:
            if "appartement" in property_type:
                item_loader.add_value("property_type","apartment")
            if "maison" in property_type:
                item_loader.add_value("property_type","house")
        adres=response.meta.get("adres")
        if adres:
            item_loader.add_value("address",adres)
        city=response.meta.get("adres")
        if city:
            item_loader.add_value("city",city.split(",")[-1].strip())
        bedroom=response.meta.get("bedroom")
        if bedroom:
            item_loader.add_value("room_count",bedroom)
        bathroom=response.meta.get("bathroom")
        if bathroom:
            item_loader.add_value("bathroom_count",bathroom)
        images_id=item_loader.get_output_value("external_id")
        images_url=f"https://www.remax.fr/Api/Listing/ListingPictures?listingTitle={images_id}"
        if images_url:
             yield Request(images_url,callback = self.images,meta={'item_loader':item_loader,})
    def images(self,response):
        item_loader=response.meta.get('item_loader')
        data=json.loads(response.body)
        images=[response.urljoin(x) for x in data['listingPictures']]
        if images:
            item_loader.add_value("images",images)
        square_meters=str(data['totalArea'])
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0])

        yield item_loader.load_item()

        
        
        # ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='property-gallery']//@src", input_type="M_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="TASSOU GESTION", input_type="VALUE")
        # ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0147241932", input_type="VALUE")
        # ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="service.immo@tassou-gestion.com", input_type="VALUE")
        
        

