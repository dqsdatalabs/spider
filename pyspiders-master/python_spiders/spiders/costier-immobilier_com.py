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
import dateparser


class MySpider(Spider):
    name = 'costier_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Costier_Immobilier_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.costier-immobilier.com/location/appartement?prod.prod_type=appt",
                ],
                "property_type":"apartment"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='_ap0eae']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)


    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)

        item_loader.add_value("external_link", response.url)

        if response.xpath("//div[text()='Loué']").get():
            return 

        images = response.xpath("//div[contains(@class,'_h6joaj image _1yfus1e')]//img/@data-src").getall()
        if images:
            item_loader.add_value("images",images)

        rent = response.xpath("//p[@class='_10hdced _5k1wy textblock ']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace(" ",""))

        item_loader.add_value("currency","EUR")

        title = response.xpath("//span[@class='_1ufrry0 undefined textblock ']/text()").get()
        if title:
            item_loader.add_value("title",title)


            city = title.split("-")[-1].split()[0]
            zipcode = title.split("-")[-1].split()[-1]
            address = title.split("-")[-1]
            if address:
                item_loader.add_value("address",address)
            if city:
                item_loader.add_value("city",city)
            if zipcode:
                item_loader.add_value("zipcode",zipcode)

        square = response.xpath("//span[contains(text(),'Surface :')]/following-sibling::span[@class='_1ufrry0 _12s20b7  textblock ']/text()").get()
        if square:
            item_loader.add_value("square_meters",square)

        room = response.xpath("//span[contains(text(),'Pièces')]/following-sibling::span[@class='_1ufrry0 _12s20b7  textblock ']/text()").get()
        if room:
            item_loader.add_value("room_count",room)

        elevator = response.xpath("//span[contains(text(),'Ascenseur :')]/following-sibling::span[@class='_1ufrry0 _12s20b7  textblock ']/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)

        desc = response.xpath("//span[@class='_1i16ls _5k1wy textblock ']/text()").get()
        if desc:
            item_loader.add_value("description",desc)

        id = response.xpath("//span[text()='Référence']/following-sibling::span/text()").get()
        if id:
            item_loader.add_value("external_id",id)

        bath = response.xpath("""//span[text()="Salle d\'eau"]/following-sibling::span/text()""").get()
        if bath:
            item_loader.add_value("bathroom_count",bath)


        floor = response.xpath("//span[text()='Étage']/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)

        item_loader.add_value("landlord_name","COSTIER IMMOBILIER")
        item_loader.add_value("landlord_phone","01 75 50 14 87")
        item_loader.add_value("property_type","apartment")

        additional_info = response.xpath("//span[@class='_1ufrry0 _5k1wy textblock ']/text()").get()
        if additional_info:
            utilities = re.search("([\d.]+) €/mois", additional_info).group(1)
            deposit = re.search("([\d.]+) €\.",additional_info).group(1)
            item_loader.add_value("utilities",utilities.split(".")[0])
            item_loader.add_value("deposit",deposit.split(".")[0])

        furnished = response.xpath("//span[text()='Entièrement meublé']").get()
        if furnished:
            item_loader.add_value("furnished",True)


        yield item_loader.load_item()  