# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
import math
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
import dateparser

class MySpider(Spider):
    name = 'toulouse_leclubdelimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://toulouse.leclubdelimmo.fr/?location-appartement-maison-loft-toulouse-haute-garonne_&a=o&id=ZY04iYlbdB"
            }
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse)


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='recherche2']/div/table"):
            follow_url = response.urljoin(item.xpath(".//tr[1]//a/@href").get())
            prop_type = item.xpath(".//tr[3]//span[2]/text()").get()
            room_count = item.xpath(".//span[@class='typo']/text()[1]").get()
            room_count = room_count.split('chambre')[0].split(',')[-1].strip()
            address = item.xpath(".//td[contains(.,'Ville')]/span[2]/text()").get()
            property_type = ""
            if "appartement" in prop_type.lower():
                property_type = "apartment"
            elif "maison" in prop_type.lower():
                property_type = "house"
            elif "studio" in prop_type.lower():
                property_type = "apartment"
            elif "duplex" in prop_type.lower():
                property_type = "apartment"
            elif "villa" in prop_type.lower():
                property_type = "house"
            elif "immeuble" in prop_type.lower():
                property_type = "house"
            if property_type != "":
                yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type, 'room_count' : room_count, "address":address})
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = "".join(response.xpath("//span[@class='titre']//text()").extract())
        item_loader.add_value("title", title.strip())

        item_loader.add_value("external_link", response.url.split("&id")[0])
        
        item_loader.add_xpath("external_id", "substring-after(//table//span[contains(.,'offre')]/text(),' ')")

        item_loader.add_value("external_source", "Toulouseleclubdelimmo_PySpider_"+ self.country + "_" + self.locale)
        
        address = response.meta.get("address")
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(" ")[1])
            item_loader.add_value("city", address.split(" ")[0])

        latitude_longitude = response.xpath("//script[contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('var lat=')[2].split(';')[0].strip()
            longitude = latitude_longitude.split('var lon=')[2].split(';')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        square_meters = response.xpath("//div[@style='height:30px;margin-top:14px']/span[@class='typo'][1]/text()[1]").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('-')[-1].split('m')[0].strip())))
            item_loader.add_value("square_meters", square_meters)

        # room_count = response.meta.get("room_count")
        # if room_count:
        #     room_count = room_count.strip().split(' ')[0]
        #     item_loader.add_value("room_count", room_count)

        date = response.xpath("//div/span[contains(.,'Loyer')]/following-sibling::span[contains(.,'Caution')]/b/text()").get()
        if date:
            if "DE SUITE" not in date:
                item_loader.add_value("available_date", datetime.strptime(date, "%d/%m/%Y").strftime("%Y-%m-%d"))
            else:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))

        utilities = response.xpath("substring-after(//div/span[contains(.,'Loyer')]/following-sibling::span[1]//text(),'+')").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace("€","").strip())

        deposit_text = response.xpath("//span[@class='typo']/b/font/font/text()").get()
        if deposit_text:
            deposit_text = deposit_text.split(" ")[0]
            price = response.xpath("substring-before(//div/span[contains(.,'Loyer')]/following-sibling::span[1]//text(),'+')").get()
            deposit = int(price.replace("€","").strip()) * int(deposit_text)
            item_loader.add_value("deposit", deposit)

        rent = response.xpath("//div[@style='float:right;width:125px;margin:0px;padding:0px;padding-top:6px']/span[@class='typo'][2]/b/text()").get()
        if rent:
            rent = rent.split('+')[0].split('€')[0].strip()
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        description = response.xpath("//b[contains(.,'Description')]/parent::span/following-sibling::span/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        available_date = response.xpath("//div[@style='float:right;width:125px;margin:0px;padding:0px;padding-top:6px']/span[@class='typo'][4]/b/text()").get()
        if available_date:
            if len(available_date.split('-')) > 2 or len(available_date.split('.')) > 2 or len(available_date.split('/')) > 2:
                if available_date.isalpha() != True:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[@style='margin:0px;padding:0px 0px 0px 18px']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        energy_label = response.xpath("//div[@style='height:30px;margin-top:14px']/span[contains(@style,'solid 1px')]/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            item_loader.add_value("energy_label", energy_label)

        landlord_phone = response.xpath("//span[contains(text(),'contactez')]/span/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        
        item_loader.add_value("landlord_name", "CLUB DE L'IMMO")

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data