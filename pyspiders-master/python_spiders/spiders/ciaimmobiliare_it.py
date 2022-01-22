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
    name = 'ciaimmobiliare_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Ciaimmobiliare_PySpider_italy"
    start_urls = ['https://ciaimmobiliare.it/wp-json/myhome/v1/estates?currency=any']  # LEVEL 1

    formdata = {
        "data[property-type][slug]": "property-type",
        "data[property-type][baseSlug]": "property_type",
        "data[property-type][key]": "property-type",
        "data[property-type][units]": "",
        "data[property-type][compare]": "=",
        "data[property-type][values][0][name]": "Appartamenti",
        "data[property-type][values][0][value]": "appartamenti",
        "data[offer-type][slug]": "offer-type",
        "data[offer-type][baseSlug]": "offer_type",
        "data[offer-type][key]": "offer-type",
        "data[offer-type][units]": "",
        "data[offer-type][compare]": "=",
        "data[offer-type][values][0][name]": "Locazione Abitativa",
        "data[offer-type][values][0][value]": "locazione-abitativa",
        "page": "1",
        "limit": "6",
        "sortBy": "newest",
        "currency": "any"
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "Appartamenti",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "stanze-singole"
                ],
                "property_type": "room"
            },
            {
                "url": [
                    "Ville / Immobili Indipendenti",
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                self.formdata["data[property-type][values][0][name]"] = item
                self.formdata["data[property-type][values][0][value]"] = item
                yield FormRequest(
                    url=self.start_urls[0],
                    dont_filter=True,
                    callback=self.parse,
                    formdata= self.formdata,
                    meta={'property_type': url.get('property_type'), "type": item}
                )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)["results"]
        
        page = response.meta.get('page', 2)
        seen = False
        for item in data:
            follow_url = f"https://ciaimmobiliare.it/immobili/{item['slug']}"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            self.formdata["data[property-type][values][0][name]"] = response.meta.get('type')
            self.formdata["data[property-type][values][0][value]"] = response.meta.get('type')
            self.formdata["page"] =f"{page}"
            yield FormRequest(
                url=self.start_urls[0],
                dont_filter=True,
                callback=self.parse,
                formdata= self.formdata,
                meta={
                    "page": page+1, 
                    "property_type": response.meta.get('property_type'),
                    "type": response.meta.get('type')
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//div[@class='mh-layout']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)

        city=response.xpath("(//li[contains(.,'PROVINCIA')]//a//text())[1]").get()
        if city:
            item_loader.add_value("city",city.replace("\r","").replace("\n","").replace("\t",""))

        address=response.xpath("(//li[contains(.,'COMUNE')]//a//text())[1]").get()
        if address:
            item_loader.add_value("address",address.replace("\r","").replace("\n","").replace("\t",""))

        external_id=response.xpath("(//li[contains(.,'RIFERIMENTO')]//following-sibling::text())[1]").get()
        if external_id:
            item_loader.add_value("external_id",external_id.replace("\r","").replace("\n","").replace("\t",""))

        rent=response.xpath("//div[@class='mh-estate__details__price__single']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("/mese")[0].replace("€","").strip())
        item_loader.add_value("currency","EUR")
        square_meters=response.xpath("//strong[contains(.,'DIMENSIONI')]/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("mq")[0])
        room=response.xpath("//strong[contains(.,'NUMERO DI VANI')]/following-sibling::a/text()").get()
        if room:
            room=re.findall("\d+",room)
            if room:
                item_loader.add_value("room_count",room)
        desc=response.xpath("//h3[.='Dettagli']/following-sibling::p/text()").getall()
        if desc:
            item_loader.add_value("description",desc)

        images=[x.split("-image:url(")[-1].split(");")[0] for x in response.xpath("//div[@class='swiper-slide']//a//@href").getall()]
        if images:
            item_loader.add_value("images",images)

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//a[@class='mh-estate__plan-thumbnail-wrapper mh-popup']//@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude_longitude = response.xpath("//estate-map[@id='myhome-estate-map']").get()

        if latitude_longitude:
            # print("//////////",latitude_longitude)
            latitude = latitude_longitude.split('"lat":')[1].split(',')[0]

            longitude = latitude_longitude.split('"lng":')[1].split('}')[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        
        furnished=response.xpath("//li[contains(.,'CARATTERISTICHE')]//following-sibling::text()[contains(.,'Arredato')]").get()
        if furnished:
            item_loader.add_value("furnished",True)
        else:
            item_loader.add_value("furnished",False)
            
        elevator=response.xpath("//li[contains(.,'CARATTERISTICHE')]//following-sibling::text()[contains(.,'Ascensore')]").get()
        if elevator:
            item_loader.add_value("elevator",True)
        else:
            item_loader.add_value("elevator",False)

        phone=response.xpath("//div[@class='mh-estate__agent__phone']//a//span//text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        name=response.xpath("//h3[@class='mh-widget-title__text']//a//span//text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        email=response.xpath("//div[@class='mh-estate__agent__email']//a//@href").get()
        if email:
            item_loader.add_value("landlord_email",email)

        yield item_loader.load_item()