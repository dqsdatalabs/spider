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
import re


class MySpider(Spider):
    name = 'brockhoff_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source='Brockhoff_PySpider_netherlands_nl'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://brockhoff.nl/Woning/Pagina/1/KoopHuur/170/TypeWoning/2#OverzichtSection",
                "property_type" : "apartment"
            },
            {
                "url" : "https://brockhoff.nl/Woning/Pagina/1/KoopHuur/170/TypeWoning/1#OverzichtSection",
                "property_type" : "house"
            },
        ]# LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='overzicht']/article/div[contains(@class,'adreskolom kolom')]"):
            status = item.xpath("./div[contains(@class,'detail')]/div/span[@class='status']/text()").get()
            if status and "verhuurd" in status.lower().strip():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@class='next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if ".html" in response.url:
            item_loader.add_value("external_source", "Brockhoff_PySpider_" + self.country + "_" + self.locale)
            
            title = "".join(response.xpath("//span[contains(@id,'AdresContainer')]//text()").getall())
            if title:
                title = re.sub('\s{2,}', ' ', title.strip())
                item_loader.add_value("title", title)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_id", (response.url).strip("/").split("/")[-2])

            
            description ="".join(response.xpath("//div[@class='beschrijvingtekst']/text()").getall())
            if description:
                description = re.sub('\s{2,}', ' ', description.strip())
                item_loader.add_value("description", description.strip())
                
            latLng = response.xpath("//script[contains(.,'woningMarkersXML ')]/text()").get()
            if latLng:
                latitude = latLng.split("<lat>")[1].split("</lat>")[0]
                longitude = latLng.split("<lng>")[1].split("</lng>")[0]
                if latitude and longitude:
                    item_loader.add_value("latitude", latitude)
                    item_loader.add_value("longitude", longitude)

            address = response.xpath("//span[@id='Content_Content_Adres']/text()").get()
            city = response.xpath("//span[@id='Content_Content_Plaats']/text()").get()
            if address:
                item_loader.add_value("address", address)
            if city:
                item_loader.add_value("city", city)

            zipcode = response.xpath("//script[contains(.,'objectZipcode')]/text()").get()
            if zipcode:
                zipcode = zipcode.split("objectZipcode': '")[1].split("'")[0]
                item_loader.add_value("zipcode", zipcode)

            item_loader.add_value("property_type", response.meta.get('property_type'))
            
            square_meters = response.xpath("//tr[./td[.='Woonoppervlakte']]/td[2]/text()").get()
            if square_meters:
                square_meters = square_meters.replace("\xa0m²","")
            item_loader.add_value("square_meters", square_meters)
            

            room_count = response.xpath("//tr[./td[.='Slaapkamers']]/td[2]/text()").get()
            item_loader.add_value("room_count", room_count)
            item_loader.add_xpath("utilities", "substring-after(substring-before(//tr[td[.='Servicekosten']]/td[2],','),'€ ')")
            
            

            images = [response.urljoin(x) for x in response.xpath("//div[@class='fotoscontainer']//a/@href").getall()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", str(len(images)))
            

            floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='PlattegrondenSlider']//a/@href").getall()]
            if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images)
            

            price = response.xpath("//tr[./td[.='Huurprijs']]/td[2]/text()").get()
            if price:
                price = price.split(",")[0].strip("€").strip()

            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        

            floor = response.xpath("//tr[./td[.='Woonlagen']]/td[2]/text()").get()
            if floor:
                item_loader.add_value("floor", floor)

            
            parking = response.xpath("//tr[./td[.='Garage']]/td[2]/text()").get()
            if parking:
                if "geen" not in parking and "neen" not in parking:
                    item_loader.add_value("parking", True)
                else:
                    item_loader.add_value("parking", False)
                
            landlord_phone = response.xpath("//div//span[@class='telefoon']/a/text()").get()
            if landlord_phone:
                item_loader.add_value("landlord_phone",landlord_phone)
            item_loader.add_xpath("landlord_name", "//div[@class='makelaarnaam']//text()")
            item_loader.add_value("landlord_email", "info@brockhoff.nl")


            yield item_loader.load_item()