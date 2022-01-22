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
import json
import re  
import dateparser  

class MySpider(Spider):
    name = "vastgoedsinnaeve_be" 
    execution_type = 'testing'
    country = 'belgium'
    locale='nl'
    external_source='Vastgoedsinnaeve_PySpider_belgium'
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.vastgoedsinnaeve.be/te-huur?MinimumSoldPeriod=&SoldPeriod=14&MinimumRentedPeriod=&RentedPeriod=14&FlowStatus=&ExcludeProjectRelatedItems=&IncludeProjectModelDefaultTransactions=&EstateTypes=&OpenHouse=&Categories=&marketingtypes-excl=&reference-notlike=&TransactionType=Rent&sorts%5B%5D=Flat&price-from=&price-to=&showMap=&NewEstate=&marketingtypes=&Reference=",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.vastgoedsinnaeve.be/te-huur?MinimumSoldPeriod=&SoldPeriod=14&MinimumRentedPeriod=&RentedPeriod=14&FlowStatus=&ExcludeProjectRelatedItems=&IncludeProjectModelDefaultTransactions=&EstateTypes=&OpenHouse=&Categories=&marketingtypes-excl=&reference-notlike=&TransactionType=Rent&sorts%5B%5D=Dwelling&price-from=&price-to=&showMap=&NewEstate=&marketingtypes=&Reference=",
                "property_type" : "house"
            },
        ] # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse, meta={'property_type': url.get('property_type')})
    def parse(self, response):
        for item in  response.xpath("//article[@class='equalheight artPub']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        next_page = response.xpath("//div[@class='paging-next paging-box']/a/@href").get()
        if next_page:        
            yield Request(url=response.urljoin(next_page),callback=self.parse,meta={'property_type': response.meta.get('property_type')})
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type",response.meta.get('property_type'))
        
        title=response.xpath("//div[@class='col-md-4']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)
        address="".join(response.xpath("//td[.='Adres:']/following-sibling::td//text()").getall())
        if address:
            item_loader.add_value("address",address)
        external_id=response.xpath("//td[.='Referentie:']/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        rent=response.xpath("//td[.='Huurprijs:']/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].replace(".",""))
        item_loader.add_value("currency","EUR")
        description="".join(response.xpath("//div[@class='divTxt']//text()").getall())
        if description:
            item_loader.add_value("description",description)
      
        images=[x for x in response.xpath("//picture/parent::a/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        square_meters=response.xpath("//label[.='Bewoonbare opp.']/preceding-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip())
        bathroom_count=response.xpath("//label[.='Badkamers']/preceding-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        room_count=response.xpath("//label[.='Slaapkamers']/preceding-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        if not room_count:
            room=response.xpath("//td[.='Type:']/following-sibling::td/text()").get()
            if room and room=="Studio":
                item_loader.add_value("room_count","1")

        
        parking=response.xpath("//label[.='Garage']/preceding-sibling::i/@class").extract()
        if parking and "close":
            item_loader.add_value("parking",False)
        if parking and "check":
            item_loader.add_value("parking",True)
        terrace=response.xpath("//label[.='Tuin']/preceding-sibling::i/@class").extract()
        if terrace and "close":
            item_loader.add_value("terrace",False)
        if terrace and "check":
            item_loader.add_value("terrace",True)
        latitude=response.xpath("//div[@id='map']/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude",latitude.replace("[","").replace("]",""))
        longitude=response.xpath("//div[@id='map']/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude",longitude.replace("[","").replace("]",""))
        landlord_name=response.xpath("//a[contains(@href,'mailto')]/preceding-sibling::h3/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)
        landlord_email=response.xpath("//a[contains(@href,'mailto')]/@href").get()
        if landlord_email:
            item_loader.add_value("landlord_email",landlord_email.split(":")[-1])
        landlord_phone=response.xpath("//a[contains(@href,'tel')]/@href").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone",landlord_phone.split(":")[-1])

        yield item_loader.load_item()