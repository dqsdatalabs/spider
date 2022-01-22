# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json
import re

class MySpider(Spider):
    name = 'wooove_nl'
    start_urls = ['https://hurenbijwooove.nl/Woning/Pagina/1'] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1


    def parse(self, response): 
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'woningList')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page < 10:
            url = f"https://hurenbijwooove.nl/Woning/Pagina/{page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})


    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Wooove_PySpider_" + self.country + "_" + self.locale)

        title = " ".join(response.xpath("//div[contains(@class,'adresregel')]/h1//text()").extract())
        title = re.sub('\s{2,}', ' ', title)
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        dontallow=response.xpath("//div[@class='ribbon-red']/text()[.='Verhuurd ovb!']").get()
        if dontallow:
            return 
        dontallow1=response.xpath("//div[@class='ribbon-red']/text()[.='Verhuurd!']").get()
        if dontallow1:
            return
        externalid=response.url
        if externalid:
            item_loader.add_value("external_id",externalid.split("/teh")[0].split("/")[-1])

        price = response.xpath("//tr[@id='Main_HuurprijsCompleet']/td[2]/text()").get()
        if price:
            item_loader.add_value(
                "rent", price.split("€")[1].split(",")[0])
        else:
            price = response.xpath("//div[@id='Main_Aanbiedingtekst']/text()[contains(.,'Huurprijs')]").get()
            if price:
                item_loader.add_value("rent", price.split("€")[1].strip().split(",")[0].strip())
        item_loader.add_value("currency", "EUR")


        square = response.xpath(
            "//tr[@id='Main_Woonopp']/td[2]/text()"
        ).get()
        if square:
            item_loader.add_value(
                "square_meters", square.split("m²")[0]
            )

        prop = response.xpath("//tr[@id='Main_KeuzeTypeWoning']/td[2]/text()").get()
        if prop:
            if "eengezinswoning" in prop:
                item_loader.add_value("property_type", "house")
            else:
                item_loader.add_value("property_type", "apartment")


        desc = "".join(response.xpath("//div[@id='Main_Aanbiedingtekst']/text()").extract())
        item_loader.add_value("description", desc.strip())

        
        room_count = response.xpath(
            "//tr[@id='Main_Aantal_kamers']/td[2]/text()"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        utilities = response.xpath("//tr[@id='Main_Servicekosten']/td[2]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[1].split(",")[0])

        street = response.xpath("//span[@id='Main_Adres']/text()").get()
        city = response.xpath("//span[@id='Main_Plaats']/text()").get()
        zipcode = response.xpath("//span[@id='Main_Postcode']/text()").get()
        item_loader.add_value("address", street + " " + zipcode + " " + city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
            
       
        dishwasher = response.xpath(
            "//div[@id='Main_Aanbiedingtekst']/text()[contains(.,'vaatwasser')]"
        ).get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        furnished = response.xpath(
            "//div[@class='interior']/small[contains(.,'Furnished')]"
        ).get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath(
            "//div[@id='Main_Aanbiedingtekst']/text()[contains(.,'Parkeren')]").get()
        if parking:
            item_loader.add_value("parking", True)

       
        balcony = response.xpath(
            "//div[@id='Main_Aanbiedingtekst']/text()[contains(.,'balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        
        item_loader.add_xpath("energy_label", "normalize-space(//div[@class='rightcontainer']/div[@class='row']/div[contains(.,'Energylabel')]/following-sibling::div/text())")

        
        images = [response.urljoin(x) for x in response.xpath(
            "//div[@class='sp-slide']//a//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            
        floor_images = [
            response.urljoin(x)
            for x in response.xpath(
                "//img[@id='Main_PlattegrondenLijst_Extrafoto_0']/@src"
            ).extract()
        ]
        if floor_images:
            item_loader.add_value("floor_plan_images", floor_images)

        lat_long= response.xpath("//script[contains(.,'GoogleKaart')]/text()").get()
        if lat_long:
            item_loader.add_value("latitude", lat_long.split("lat>")[1].split("</")[0].strip())
            item_loader.add_value("longitude", lat_long.split("lng>")[1].split("</")[0].strip())

        item_loader.add_value("landlord_phone", "020 2615612")
        item_loader.add_value("landlord_name", "Wooove")
        item_loader.add_value("landlord_email", "info@wooove.nl")

        
        
        yield item_loader.load_item()