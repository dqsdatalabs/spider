# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser


class MySpider(Spider):
    name = 'huisportaal_nl'
    start_urls = [
        "https://www.huisportaal.nl/huurwoningen"
    ]  # LEVEL 1
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    
    def parse(self, response):
        for i in range(1,6):
            headers = {
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
                "origin": "https://www.huisportaal.nl"
            }
           
            data = {
                 "frmAction": "showHousesFilter",
                  "page": f"{i}",
                  "type": "rent",
                  "number": "10"
           }
           
            yield FormRequest(
                "https://www.huisportaal.nl/showHousesFilter",
                formdata=data,
                dont_filter=True,
                headers=headers,
                callback=self.jump,
            )
    
    # 1. FOLLOWING
    def jump(self, response):
        for item in response.xpath("//div[@class='woning']/div[contains(@class,'huisfoto1')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Huisportaal_PySpider_" + self.country + "_" + self.locale)
        rented = response.xpath("//div[@id='media']/div[@class='rent2']/@class").extract_first()
        if rented:
            return

        title = response.xpath("//h1/text()").extract_first()
        item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", (response.url).split("/")[-1].split("-")[0])

        desc = "".join(response.xpath("//div[@id='samenvatting']/p/text()").extract())
        item_loader.add_value("description", desc.strip())
        
        item_loader.add_value("address", title)
        item_loader.add_value("city", title.split(",")[1].strip())

        #{'appartement': 'OK', 'woning': 'OK', 'parking': 'OK', 'andere': 'OK'}
        property_type = response.xpath("//li[./div='Woontype 1']/div[2]/text()").get()
        if property_type and "appartement" in property_type:
            item_loader.add_value("property_type", "apartment")
        elif property_type and "woning" in property_type:
            item_loader.add_value("property_type", "house")
        else:
            return
        
        square_meters = response.xpath("//li[contains(.,'woonopp')]//text()").get()
        if square_meters:
            square_meters = square_meters.strip("m2")
        item_loader.add_value("square_meters", square_meters)
        

        room_count = response.xpath("normalize-space(//li[div[.='Aantal kamers']]/div[2]/text())").get()
        if room_count and "0" not in room_count:    
            item_loader.add_value("room_count", room_count.strip().split("kamers")[0].strip())
        
        item_loader.add_xpath("bathroom_count", "normalize-space(//ul/li[div[.='Aantal badkamers']]/div[2]/text())")
        utilities = response.xpath("substring-after(substring-before(//li[div[.='Totale servicekosten']]/div[2],'per'),'€ ')").extract_first().strip()
        if utilities != '0':
            item_loader.add_value("utilities",utilities)

        available_date = response.xpath("//li[./div='Aangeboden sinds']/div[2]/text()").get()
        if available_date and available_date.replace(" ","").isalpha() != True:
            date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        

        images = [response.urljoin(x) for x in response.xpath("//div[@id='media']/div[@id='fotos']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='plattegrond']/div[@id='fotos']/a/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)  
        
        price = response.xpath("//div[contains(text(),'€') and contains(@class,'prijs')]/text()").get()
        if price:
            price = price.split(" ")[1].strip()

        item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("substring-after(//li[./div='Waarborgsom']/div[2]/text(),'€ ')").get()
        if deposit != '0':
            item_loader.add_value("deposit", deposit)
        
        furnished = response.xpath("//li[./div='Uitvoering']/div[2]/text()").get()
        if furnished:
            if furnished.lower() == "gestoffeerd" or "Gemeubileerd" in furnished:
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
    
        energy_label = response.xpath("//li[./div='Klasse']/div[2]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        floor = response.xpath("//li[./div='Aantal woonlagen']/div[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        terrace = response.xpath("//li[./div='Zolder']/div[2]/text()").get()
        if terrace:
            if terrace.lower() == "ja":
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)
 
        parking = response.xpath("//li[./div='Garage']/div[2]/text()").get()
        if parking:
            if parking.lower() == "ja":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
        item_loader.add_value("landlord_phone", "085-782.00.00")
        item_loader.add_value("landlord_email", "info@huisportaal.nl")
        item_loader.add_value("landlord_name", "Huis Portaal")
        yield item_loader.load_item()