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
import math
class MySpider(Spider):
    name = 'nowodworskiestates_pl'
    execution_type='testing'
    country='poland'
    locale='pl'
    external_source="Nowodworskiestates_PySpider_poland"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.nowodworskiestates.pl/wyszukiwarka/?lat=&lon=&radiusTotal=&sort=created_desc&roomsFrom=&roomsTo=&yearFrom=&yearTo=&floorNoFrom=&floorNoTo=&priceM2From=&priceM2To=&parkingPlace=&transactionType=rent&propertyType=home&mortgageMarket=&keywords=&location=&radius=&priceFrom=&priceTo=&areaFrom=&areaTo=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.nowodworskiestates.pl/wyszukiwarka/?lat=&lon=&radiusTotal=&sort=created_desc&roomsFrom=&roomsTo=&yearFrom=&yearTo=&floorNoFrom=&floorNoTo=&priceM2From=&priceM2To=&parkingPlace=&transactionType=rent&propertyType=flat&mortgageMarket=&keywords=&location=&radius=&priceFrom=&priceTo=&areaFrom=&areaTo=",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//a[@class='gtag_event_search']/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            seen = True
        if page == 2 or seen: 
            nextpage=f"https://www.nowodworskiestates.pl/wyszukiwarka/?lat=&lon=&radiusTotal=0&sort=created_desc&roomsFrom=&roomsTo=&yearFrom=&yearTo=&floorNoFrom=&floorNoTo=&priceM2From=&priceM2To=&parkingPlace=&transactionType=rent&propertyType=flat&mortgageMarket=&keywords=&location=&radius=&priceFrom=&priceTo=&areaFrom=&areaTo=&pageNumber={page}" 
            if nextpage:      
                yield Request(
                    response.urljoin(nextpage),
                    callback=self.parse,
                    dont_filter=True,
                    meta={"page":page+1,"property_type":response.meta["property_type"]})
        
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        external_id=response.xpath("//span[contains(.,'ID oferty')]/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.replace("\r","").replace("\n","").strip())

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//h5[contains(.,'Opłaty')]/parent::div/following-sibling::div//span[contains(.,'Cena:')]/following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("PLN")[0].replace("\n","").strip().replace(" ",""))
        item_loader.add_value("currency","PLN")
        square_meters=response.xpath("//span[contains(.,'Powierzchnia:')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0].split("m")[0].replace("\n","").strip())
        room_count=response.xpath("//span[contains(.,'Pokoje:')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[contains(.,'Łazienki:')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        balcony=response.xpath("//span[contains(.,'Balkon/taras:')]/following-sibling::span/text()").get()
        if balcony and "Tak" in balcony:
            item_loader.add_value("balcony",True)
        furnished=response.xpath("//span[contains(.,'Umeblowane:')]/following-sibling::span/text()").get()
        if furnished and "Tak" in furnished:
            item_loader.add_value("furnished",True)
        adres=response.xpath("//span[contains(.,'Adres:')]/following-sibling::span/text()").get()
        if adres:
            item_loader.add_value("address",adres.replace("\n","").strip())
        images=[x for x in response.xpath("//a//img//@data-src").getall()]
        if images:
            item_loader.add_value("images",images)
        floor_plan_images=response.xpath("//div[@class='floor-plans-info-box']/following-sibling::a/img/@src").getall()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images",floor_plan_images)
        description=response.xpath("//div[@id='film']/following-sibling::div/div[2]/div/text()").getall()
        if description:
            item_loader.add_value("description",description)
        item_loader.add_value("landlord_name","Nowodworski Estates")


        floor = response.xpath("//span[contains(text(),'Piętro:')]/following::span/text()").get()
        if floor:
            if floor.strip().isdigit():
                item_loader.add_value("floor",floor.strip())
            else:
                item_loader.add_value("floor","0")

        features = "".join(response.xpath("//span//text()").getall()).lower()
        if "pralka" in features:
            item_loader.add_value("washing_machine",True)

        utilities = response.xpath("//span[contains(text(),'administracyjny:')]/following::span/text()").get()
        if utilities:
            utilities = utilities.split("P")[0].replace(" ","")
            item_loader.add_value("utilities",utilities)


        yield item_loader.load_item()