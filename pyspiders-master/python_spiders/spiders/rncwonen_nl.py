# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser
import re
  

class MySpider(Spider):
    name = "rncwonen_nl"
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1
    external_source="Rncwonen_PySpider_netherlands_nl"
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.rncwonen.nl/nl/aanbod/&rentaltype=Appartement&place=&uitvoering=&van=&tot=", "property_type": "apartment"},
            {"url": "https://www.rncwonen.nl/nl/aanbod/&rentaltype=Kamer&place=&uitvoering=&van=&tot=", "property_type": "apartment"},
            {"url": "https://www.rncwonen.nl/nl/aanbod/&rentaltype=Studio&place=&uitvoering=&van=&tot=", "property_type": "studio"},
            {"url": "https://www.rncwonen.nl/nl/aanbod/&rentaltype=Woonhuis&place=&uitvoering=&van=&tot=", "property_type": "house"},
            {"url": "https://www.rncwonen.nl/nl/aanbod/&rentaltype=Chalet&place=&uitvoering=&van=&tot=", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url":url.get('url')})

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        for follow_url in response.css("div.huis a::attr(href)").extract():
            yield response.follow(follow_url, self.populate_item, meta={'property_type': response.meta.get('property_type')})
        yield self.paginate(response)

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        verhuurd = response.xpath("//div[@id='images']/div/img/@src[contains(.,'verhuurd')]").extract_first()
        if verhuurd:return

        item_loader.add_value("external_source",self.external_source)

        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])

        price = "".join(response.xpath("//li[span[.='Huurprijs']]/text()").extract())
        if price:
            item_loader.add_value("rent_string", price)

        deposit = response.xpath("//li[span[.='Borg']]/text()[contains(., '€')]").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€ ")[1])

        utilities = response.xpath("//li[span[.='Servicekosten']]/text()[contains(., '€')]").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€ ")[1])

        item_loader.add_value("property_type", response.meta.get('property_type'))

        square = response.xpath("//li[span[.='Woonoppervlakte']]/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m")[0].replace(":","").strip())

        images = [response.urljoin(x)for x in response.xpath("//div[@id='imagelist']/div/a/@href").extract()]
        if images:
                item_loader.add_value("images", images)

        room = "".join(response.xpath("//li[span[.='Aantal slaapkamers']]/text()").extract()).replace(":","")
        if room:
            room = room.strip()
            if "studio" in item_loader.get_collected_values("property_type") and "0" in room:
                item_loader.add_value("room_count", "1")
            elif "room" in item_loader.get_collected_values("property_type") and "0" in room:
                item_loader.add_value("room_count", "1")
            else:
                if "0" not in room:
                    item_loader.add_value("room_count", room)

        floor = "".join(response.xpath("//li[span[.='Verdieping']]/text()").extract()).replace(":","")
        item_loader.add_value("floor",floor.strip())

        available_date = "".join(response.xpath("//li[span[.='Aanvaarding']]/text()[. !=': Per direct']").extract())
        if available_date:
            date_parsed = dateparser.parse(available_date.strip().replace(":",""), date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        desc = "".join(response.xpath("//div[@id='right']/h2[contains(.,'Omschrijving')]/following-sibling::p//text()").extract())
        description = re.sub('\s{2,}', ' ', desc.strip())
        item_loader.add_value("description", description)

        city = "".join(response.xpath("//li[span[.='Plaats']]/text()").extract())
        item_loader.add_value("city",city.split(":")[1].strip())

        street = response.xpath("//ul/li[contains(.,'Straat')]/text()").get().split(":")[1].strip()
        province = response.xpath("//ul/li[contains(.,'Provincie')]/text()").get().split(":")[1].strip()

        item_loader.add_value("address", street + ", " + city.split(":")[1].strip() + ", " + province)

        washing_machine = "".join(response.xpath("//div[@id='right']/p/text()[contains(.,'wasmachine')]").extract()).strip()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        furnished = "".join(response.xpath("substring-after(//li[span[.='Uitvoering']]/text(),': ')").extract()).strip()
        if furnished == "Gestoffeerd":
            item_loader.add_value("furnished", True)
        elif furnished=="Gemeubileerd":
            item_loader.add_value("furnished",True)
        else:
            item_loader.add_value("furnished", False)

        item_loader.add_value("landlord_phone", "+31 (0)73 851 76 20")
        item_loader.add_value("landlord_email", "info@rncwonen.nl")
        item_loader.add_value("landlord_name", "RNC Wonen")

        yield item_loader.load_item()

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.css(
            "div#nav a::attr(href)"
        ).extract_first()  # pagination("next button") <a> element here
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse, meta={'property_type': response.meta.get('property_type')})
