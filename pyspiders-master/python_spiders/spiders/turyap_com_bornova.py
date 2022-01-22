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
    name = 'turyap_com_bornova'

    execution_type='testing'
    country='turkey'
    locale='tr'
    thousand_separator =","
    scale_separator ="."

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.turyap.com.tr/arama?st=2&mg=1&sg=1|9&fo=470019&sort=4",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.turyap.com.tr/arama?st=2&mg=1&sg=8|10|11|13&fo=470019&sort=4",
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='wrapper']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Turyap_com_bornova_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//h1[@class='h1']/text()")
        item_loader.add_xpath("external_id", "//label/strong/text()")

        price = "".join(response.xpath("//span[@class='oripri price']/text()").extract())
        if price:
            item_loader.add_value("rent_string", price)

        deposit = "".join(response.xpath("//div[span[.='Depozit']]/span[2]/text()[.='0']").extract())
        if deposit:
            item_loader.add_value("deposit", deposit)

        utilities = "".join(response.xpath("//div[span[.='Aidat']]/span[2]/text()[.='0']").extract())
        if utilities:
            item_loader.add_value("utilities", utilities)

        images = [response.urljoin(x)for x in response.xpath("//img[@class='thumb photo']/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        city =  "".join(response.xpath("//div[@class='row']/div/h4/text()").extract())
        if city:
            if "/" in city:
                city = city.split("/")[0]
            item_loader.add_value("city", city.strip())
        address =  "".join(response.xpath("//div[@class='row']/div/h4//text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
            

        square_meters = "".join(response.xpath("//span[@class='area'][1]/text()").extract())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0])


        item_loader.add_xpath("external_id", "//tr[td[.='İlan No']]/td[2]/text()")
        item_loader.add_xpath("floor", "//div[span[.='Bulunduğu Kat']]/span[2]/text()")

        room_count =  response.xpath("//span[contains(.,'Oda Sayısı')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        item_loader.add_xpath("bathroom_count", "//div[span[. ='Banyo Sayısı']]/span[2]/text()")
        desc = "".join(response.xpath("//div[@id='product_intro']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "balkon" in desc.lower():
                item_loader.add_value("balcony", True)
            if "eşyalı" in desc.lower() or "EŞYALI" in desc:
                item_loader.add_value("furnished", True)
        dishwasher = response.xpath("//div[@id='product_intro']//text()[contains(.,'Bulaşıkmakinesi') or contains(.,'Bulaşık makinesi')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        washing_machine = response.xpath("//div[@id='product_intro']//text()[contains(.,'Çamaşır Makinesi')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        swimming_pool = "".join(response.xpath("//div[@id='product_intro']//text()[contains(.,'yüzme havuzu')]").extract())
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        elevator = "".join(response.xpath("//div[@id='product_intro']//text()[contains(.,'Asansör')]").extract())
        if elevator:
            item_loader.add_value("elevator", True)

        # balcony = "".join(response.xpath("//div[@id='product_intro']//text()[contains(.,'Balkon') or contains(.,'balkon') ]").extract())
        # if balcony:
        #     item_loader.add_value("balcony", True)

        parking = "".join(response.xpath("//div[@id='product_intro']//text()[contains(.,'otopark')]").extract())
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_xpath("landlord_phone", "//div[@class='phone']//text()[normalize-space()]")
        item_loader.add_xpath("landlord_name", "//div[contains(@class,'name')]//text()")
        
        latitude_longitude = response.xpath("//script[contains(.,'$location_info')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('$location_info_lat=')[1].split(';')[0].strip()
            longitude = latitude_longitude.split('$location_info_lng=')[1].split(';')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        yield item_loader.load_item()


def split_room(room_count,get):
    count1 = room_count.split("+")[0]
    count2 = room_count.split("+")[1]
    if count2 !="": 
        count = int(count1)+int(count2)
        return str(count)
    else:
        count = int(count1.replace("+",""))
        return str(count)