# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import dateparser

class MySpider(Spider):
    name = 'immosud_net'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.immo-sud.net/fr/liste.htm?ope=2#page=1&TypeModeListeForm=text&ope=2&filtre=8", "property_type": "house"},
            {"url": "https://www.immo-sud.net/fr/liste.htm?ope=2#page=1&TypeModeListeForm=text&ope=2&filtre=2", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//ul[@class='liste-bien-buttons']/li/a[@itemprop='url']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
       
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Immosud_PySpider_"+ self.country + "_" + self.locale)

        title = "".join(response.xpath("//div[@class='detail-bien-title']/*[self::h1 | self::h2]/text()").extract())
        item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)

        external_id = response.xpath("//div[2]/span[contains(.,'Ref')]/following-sibling::span[@itemprop='productID']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
       
        result = response.xpath("//div[@class='detail-bien-title']/h2/text()").get()
        if result:
            item_loader.add_value("address", result)
            zipcode = result.strip().split('(')[1].strip(')')
            if zipcode.isdigit():
                item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", result.split("(")[0].strip())
        
        item_loader.add_value("property_type", response.meta.get('property_type'))

        square_meters = response.xpath("//div[@class='detail-bien-specs']/ul/li[1]/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(' ')[0]
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//div[@class='detail-bien-specs']/ul/li[contains(.,'pièce(s)')]/text()").get()
        if room_count:
            room_count = room_count.strip().split('pièce(s)')[0].strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[@class='detail-bien-specs']/ul/li[contains(.,'pièce(s)')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split('pièce(s)')[0].strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        price = response.xpath("//div[@class='detail-bien-prix']/text()").get()
        if price: 
            # price = price.split('€')[0].strip().replace(' ', '.')
            item_loader.add_value("rent_string", price.replace(" ",""))
       
        deposit = response.xpath("//div[@class='hidden prix-location']//li[contains(.,'Dépôt de garantie')]/span[@class='cout_honoraires_loc']//text()").get()
        if deposit: 
            item_loader.add_value("deposit", deposit.replace(" ",""))

        utilities = response.xpath("//div[@class='hidden prix-location']//li[contains(.,'provisions sur charges')]//span[@class='cout_charges_mens']/text()").get()
        if utilities: 
            item_loader.add_value("utilities", utilities.replace(" ",""))
        energy_label = response.xpath("//div[contains(@class,'detail-bien-dpe')]//img[1]/@src").get()
        if energy_label:
            try:
                energy_label = energy_label.split("consommation-")[1].split(".")[0].strip()
                if energy_label.isdigit():     
                    item_loader.add_value("energy_label", energy_label_calculate(energy_label))
            except:
                pass
        description = response.xpath("//div[@class='detail-bien-desc-content clearfix']/p[1]/span/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        if "disponible le" in desc_html.lower():
            available_date = desc_html.split("Disponible le")[1].strip().split(" ")[0]
            date_parsed = dateparser.parse(available_date)
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//div[@class='thumbs-flap-container']/div/img/@src[not(contains(.,'images/vide_detail_mini'))]").getall()]
        if images:

            item_loader.add_value("images", images)

        latitude = response.xpath("//li[contains(@class,'marker-lat')]//text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//li[contains(@class,'marker-lng')]//text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_phone", "04 67 21 39 55")
        item_loader.add_value("landlord_name", "INTER-MED-IMMO34 AGENCE D'AGDE")
        item_loader.add_value("landlord_email", "patrick-immosud@orange.fr")

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label