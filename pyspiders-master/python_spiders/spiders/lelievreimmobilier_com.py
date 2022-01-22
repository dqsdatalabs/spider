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
    name = 'lelievreimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.lelievre-immobilier.com/sites/default/files/annonces/json/allAnnonces.json?2020100907"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)

        for item in data:
            if "/location/maison" in item["url"]:
                follow_url = response.urljoin(item["url"])
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': 'house'})
            elif "/location/appartement" in item["url"]:
                follow_url = response.urljoin(item["url"])
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': 'apartment'})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Lelievreimmobilier_PySpider_"+ self.country + "_" + self.locale)

        title = " ".join(response.xpath("//div[@class='title']/h1//text()").extract())
        item_loader.add_value("title", re.sub('\s{2,}', ' ', title))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))

        external_id = "".join(response.xpath("normalize-space(//div[@class='reference']/text())").extract())
        if external_id:
            item_loader.add_value("external_id",external_id.replace("REF.","").strip())

        price = response.xpath("normalize-space(//div[@class='price']/text())").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))

        square = response.xpath("//div[div[. ='Surface habitable']]/div[@class='value']/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m")[0])
        else: 
            square = "".join(response.xpath("//div[@class='title']/h1//text()").extract())
            if square:
                square = square.split("m2")[0].strip().split(" ")[-1]
                item_loader.add_value("square_meters", square)
        
        images = [response.urljoin(x)for x in response.xpath("//div[@id='galleria']//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
 
        
        room = response.xpath("//div[div[. ='Chambres']]/div[@class='value']/text()").get()
        if room:
            item_loader.add_value("room_count",room)
        else:
            room = "".join(response.xpath("//div[@class='title']/h1//text()").extract())
            if "pièces" in room:
                room = room.split("pièces")[0].strip().split(" ")[-1]
                item_loader.add_value("room_count", room)

        item_loader.add_xpath("floor","//div[div[. ='Etage']]/div[@class='value']/text()")

        available_date = "".join(response.xpath("//div[div[contains(.,'Disponibilité')]]/div[@class='text green']/text()").extract()).strip()
        if "immédiate" not in available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        desc = "".join(response.xpath("//div[@class='section_text']/text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        item_loader.add_value("description", desc.strip())
        
        if "piscine" in desc.lower():
            item_loader.add_value("swimming_pool", True)
        
        furnished=response.xpath("//div[@class='section_text']/text()[contains(.,' meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        if "salle" in desc.lower():
            bathroom=desc.lower().split("salle")[0].strip().split(" ")[-1]
            if "une" in bathroom.lower():
                item_loader.add_value("bathroom_count", "1")
        
        date2=False
        if "disponible" in desc.lower():
            try:
                available_date=desc.lower().split("disponible")[1].replace("au","").strip().split(" ")[0]
                if available_date:
                    date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
                    date2 = date_parsed.strftime("%Y-%m-%d")
            except:
                date2=False
            if date2:
                item_loader.add_value("available_date", date2)
            
        label = response.xpath("//div[@class='scale_performance']/@data-energy[.!='VI']").extract_first()
        if label:
            item_loader.add_value("energy_label", label.upper())
        
        images = [x for x in response.xpath("//div[@id='galleria']/div//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        utilities="".join(response.xpath("//div[@class='details']/text()").getall())
        if utilities and "honoraires" in utilities.lower():
            item_loader.add_value("utilities", utilities.split(":")[1].split(".")[0])
        
        terrace = "".join(response.xpath("//div[div[. ='Commodités']]/div[@class='value']/text()[contains(.,'Parking')]").extract()).strip()
        if terrace:
            item_loader.add_value("parking", True)

        terrace = "".join(response.xpath("//div[div[. ='Commodités']]/div[@class='value']/text()[contains(.,'Balcony')]").extract()).strip()
        if terrace:
            item_loader.add_value("balcony", True)

        terrace = "".join(response.xpath("//div[div[. ='Commodités']]/div[@class='value']/text()[contains(.,'Terrasse')]").extract()).strip()
        if terrace:
            item_loader.add_value("terrace", True)

        terrace = "".join(response.xpath("//div[div[. ='Commodités']]/div[@class='value']/text()[contains(.,'Ascenseur')]").extract()).strip()
        if terrace:
            item_loader.add_value("elevator", True)

        address = "".join(response.xpath("//div[@class='title']/h1/text()[3]").extract())

        item_loader.add_value("zipcode", address.strip().split("(")[1].split(")")[0])
        item_loader.add_value("address",address)
        item_loader.add_value("city", address.strip().split("(")[0].strip())

        item_loader.add_xpath("latitude","//div[@class='map__container']/span[@id='g_lat']/@data-val")
        item_loader.add_xpath("longitude", "//div[@class='map__container']/span[@id='g_lng']/@data-val")


        item_loader.add_value("landlord_phone", "0240489339")
        item_loader.add_value("landlord_name", "Lelievre İmmobilier")
        item_loader.add_value("landlord_email", "paris.lebon@fonciere-lelievre.com")

        yield item_loader.load_item()