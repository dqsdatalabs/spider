# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser


class MySpider(Spider):
    name = 'expats_amsterdam'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' 
    external_source='Expatsamsterdam_PySpider_netherlands_nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.expats.amsterdam/apartments/?apt-type=apartment&apt-bedrooms=all&undefined=all&apt-price=all&apt-interior=either",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.expats.amsterdam/apartments/?apt-type=attached-house&apt-bedrooms=all&undefined=all&apt-price=all&apt-interior=either",
                "property_type" : "house"
            },
        ]# LEVEL 1
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for follow_url in response.xpath("//div[@id='apt-list']/div/a/@href").extract():
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(.,'More results')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)

        title = "".join(response.xpath("//h1/text()").extract())
        if title !="Parkeerplaatsen":
            item_loader.add_value("title", title)

            floor = "".join(response.xpath("//div[@id='apt-single-main-col']//p[contains(.,'floor')]/text()").extract())
            if floor:
                floor_s = floor.split("floor")[0].split(" ")[-1]
                if  floor_s:
                    item_loader.add_value("floor", floor_s)
                else:
                    floor2 = floor.split("floor")[0].split(" ")[-2]
                    if floor2:                           
                        item_loader.add_value("floor", floor2.replace("to","").replace("p",""))

            item_loader.add_value("external_link", response.url)
            external_id = response.xpath("//link[@rel='shortlink']//@href").get()
            if external_id:
                item_loader.add_value("external_id", external_id.split('?p=')[1])

            address = response.xpath("//div[@id='content']/h1/text()").get()
            city = response.xpath("//strong[.='City area:']/following-sibling::text()").get()
            if address and city:
                address += ', ' + city.strip()
                item_loader.add_value("address", address)
                item_loader.add_value("city", city.replace("\t","").replace("\n",""))

            item_loader.add_value("property_type", response.meta.get('property_type'))

            square_meters = response.xpath("//strong[.='Size:']/following-sibling::text()").get()
            if square_meters:
                square_meters = square_meters.strip().split(' ')[0]
                item_loader.add_value("square_meters", square_meters)

            price = response.xpath("//strong[.='Monthly rent:']/following-sibling::text()").get()
            if price: 
                price = price.strip().split(',')[0].split(' ')[1]
                item_loader.add_value("rent", price)
                item_loader.add_value("currency", "EUR")

            date = "".join(response.xpath("//li/strong[.='Available from:']/following-sibling::text()").extract())
            if date:
                date2 =  date.strip()
                date_parsed = dateparser.parse(
                    date2, date_formats=["%m-%d-%Y"]
                )
                if date_parsed is not None:
                    date3 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date3)

            description = ""
            description_1 = response.xpath("//div[@id='apt-single-main-col']/p/text()").getall()
            if description_1:
                description_1 = [x.strip() for x in description_1]
                for x in description_1:
                    description += x + ' \n'
                item_loader.add_value("description", str(description))

            images = [x for x in response.xpath("//div[@id='apt-slider-wrap']//img/@src").getall()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", str(len(images)))

            furnished = response.xpath("//strong[.='Interior:']/following-sibling::text()").get()
            if furnished:
                if furnished.strip() == 'Furnished':
                    furnished = True
                else:
                    furnished = False
                item_loader.add_value("furnished", furnished)

            room_count = response.xpath("//strong[.='Bedrooms:']/following-sibling::text()").get()
            if room_count:
                room_count = room_count.strip()
                if room_count != "0":
                    item_loader.add_value("room_count", room_count)

            item_loader.add_value("landlord_phone", "+31 (0) 20 261 5297")
            item_loader.add_value("landlord_email", "info@expats.amsterdam")
            item_loader.add_value("landlord_name", "EXPATS.AMSTERDAM")

            yield item_loader.load_item()