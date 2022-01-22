# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
class MySpider(Spider):

    name = 'reals_pl'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source = "Reals_PySpider_poland"
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://reals.pl/lista-ofert/?f_location_locality[0]=&mapa=&mapaX1=&mapaY1=&mapaX2=&mapaY2=&mapaCX=&mapaCY=&mapaCZ=0&mapaVis=0&f_street_name=&f_sectionName1=Apartment&f_sectionName2=Rental&f_totalAreaMin=&f_totalAreaMax=&f_noOfRoomsMin=&f_noOfRoomsMax=&f_price_amountMin=&f_price_amountMax=&f_floorNoMin=&f_floorNoMax=&content_clearfix=&f_yearBuiltMin=&f_yearBuiltMax=&f_mortgageMarket=&f_commercialObjectType=&f_commercialSpaceType=&f_lotType=&f_listingId=&submit=Szukaj&offset=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://reals.pl/lista-ofert/?f_location_locality[0]=&mapa=&mapaX1=&mapaY1=&mapaX2=&mapaY2=&mapaCX=&mapaCY=&mapaCZ=0&mapaVis=0&f_street_name=&f_sectionName1=House&f_sectionName2=Rental&f_totalAreaMin=&f_totalAreaMax=&f_noOfRoomsMin=&f_noOfRoomsMax=&f_price_amountMin=&f_price_amountMax=&f_noOfFloorsMin=&f_noOfFloorsMax=&content_clearfix=&f_yearBuiltMin=&f_yearBuiltMax=&f_mortgageMarket=&f_commercialObjectType=&f_commercialSpaceType=&f_lotType=&f_listingId=&submit=Szukaj&offset=0",
                ],
                "property_type" : "house"
            }
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//div[contains(@class,'list-container')]//article//h4/a/@href").getall():
            follow_url = response.urljoin(url)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[.='»']/@href").get()
        if next_button: 
            url = "http://reals.pl/lista-ofert/" + next_button
            yield Request(url, callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta["property_type"])

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
            
        external_id = response.xpath("//ul[@class='arrow-bullet-list clearfix']//li[contains(.,'Numer oferty:')]//text()").get()
        if external_id:
            external_id = external_id.split("Numer oferty:")[1]
            external_id = external_id.replace(" ","")
            item_loader.add_value("external_id",external_id)

        description = response.xpath("//div[@class='content clearfix']//text()").getall()
        if description:
            item_loader.add_value("description",description)

        address = response.xpath("//div[@class='wrap clearfix']//h4[@class='title']//text()").getall()
        if address:
            item_loader.add_value("address",address)

        rent = response.xpath("//h5[@class='price']//span[contains(.,'PLN')]/text()").get()
        if rent:
            rent = rent.split("PLN")[0]
            rent =rent.replace(" ","") 
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","PLN")

        room_count = response.xpath("//ul[@class='arrow-bullet-list clearfix']//li[contains(.,'Liczba pokoi:')]//text()").get()
        if room_count:
            room_count = room_count.split("Liczba pokoi:")[1]
            room_count = room_count.replace(" ","")
            item_loader.add_value("room_count",room_count)

        floor = response.xpath("//ul[@class='arrow-bullet-list clearfix']//li[contains(.,'Piętro:')]//text()").get()
        if floor and "0" not in floor:
            floor = floor.split("Piętro:")[1]
            floor = floor.replace(" ","")
            item_loader.add_value("floor",floor)

        square_meters = response.xpath("//div[@class='property-meta clearfix']//span//text()[contains(.,'m2')]").get()
        if square_meters:
            square_meters = square_meters.split("m2")[0]
            square_meters = square_meters.replace(" ","")
            item_loader.add_value("square_meters",square_meters)

        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'slides')]//a[@class='swipebox']//@href").extract()]
        if images:
            item_loader.add_value("images", images)

        landlord_name = response.xpath("//div[@class='left-box']//h3//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("(//li[contains(.,'Telefon')]//a/text())[1]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//i[contains(@class,'fa fa-envelope-o')]//following-sibling::a/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)   

        yield item_loader.load_item()