# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser
import re
import json

class MySpider(Spider):
    name = "brickvastgoed_nl"
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source='Brickvastgoed_PySpider_netherlands_nl'
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www.brickvastgoed.nl",
    }
    index=0
    def start_requests(self):

        self.start_urls = [
            {"type": "1", "property_type": "apartment", "value": "Appartement"},
            {"type": "2", "property_type": "house", "value": "Hoekwoning"},
            {"type": "3", "property_type": "room", "value": "Onzelfstandige kamer"},
            {"type": "4", "property_type": "studio", "value": "Studio"},
            {"type": "5", "property_type": "house", "value": "Tussenwoning"},
            {"type": "6", "property_type": "house", "value": "Villa"},
        ]  # LEVEL 1

        self.data = {
            "filter_sort": "id",
            "aantal": "12",
            "filter_plaats": "",
            "filter_straat": "",
            "filter_prijs_van": "-1",
            "filter_prijs_tot": "1000000000",
            "filter_woningtype": self.start_urls[self.index].get("value"),
            "filter_kamers": "0",
            "filter_vorm": "",
            "filter_beschikbaarheid": "",
        }

        yield FormRequest(
            "https://www.brickvastgoed.nl/aanbod",
            formdata=self.data,
            headers=self.headers,
            callback=self.jump,
            dont_filter=True,
            meta={'property_type': self.start_urls[self.index].get('property_type')},
        )
    def jump(self, response):
        yield Request("https://www.brickvastgoed.nl/aanbod", self.parse,dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        
    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        
        for item in response.xpath("//a[@class='item'][div[@class='overlay']/img[not(contains(@src,'/verhuurd'))]]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        if page == 2 or seen:            
            p_url = f"https://www.brickvastgoed.nl/aanbod/page-{page}/"
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})
        
        else:
            self.index +=1
            if self.index < len(self.start_urls):                
                self.data["filter_woningtype"] = self.start_urls[self.index].get("value")

                yield FormRequest(
                "https://www.brickvastgoed.nl/aanbod",
                formdata=self.data,
                headers=self.headers,
                callback=self.jump,
                dont_filter=True,
                meta={'property_type': self.start_urls[self.index].get('property_type')},
            )
    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Brickvastgoed_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("id-")[1].split("/")[0])

        available_date ="".join(response.xpath("//tr[td[.='beschikbaar:']]/td[2]/text()").extract())
        if available_date:
            ava = available_date.split(" ")[-1]
            date_parsed = dateparser.parse(ava, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        price = response.xpath("//tr[td[.='Huurprijs per maand:']]/td/text()[contains(., '€')]").extract_first()
        if price:
            item_loader.add_value("rent", price.split("€")[1].split(",")[0])
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//tr[td[.='Borgsom:']]/td/text()[contains(., '€')]").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[1].split(",")[0])

        item_loader.add_value("property_type", response.meta.get('property_type'))

        square = response.xpath("//tr[td[.='Woonoppervlakte:']]/td[2]/text()").get()
        if square and square != "0 m²":
            item_loader.add_value("square_meters", square.split("m²")[0])

        images = [response.urljoin(x)for x in response.xpath("//div[contains(@class,'col-6 image')]/a/@href[not(contains(.,'/contact_referentie'))]").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_xpath("room_count","//tr[td[.='Aantal slaapkamers:']]/td[2]/text()")

        desc = "".join(response.xpath("//div[@class='block block-2']//text() | //div[@class='description-content']/p//text()").extract())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))

        energ_label = response.xpath("//tr[td[.='Energie label:']]/td/span/@class").extract_first()
        if energ_label:
            item_loader.add_value("energy_label", energ_label.split("l_")[-1])

        terrace = "".join(response.xpath("//td[@class='extras']/text()[contains(.,' Balkon')]/preceding::i[1][@class='fa fa-check']").extract())
        if terrace:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)
        terrace = "".join(response.xpath("//td[@class='extras']/text()[contains(.,'terras')]/preceding::i[1][@class='fa fa-check']").extract())
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            item_loader.add_value("terrace", False)

        terrace = response.xpath("//td[@class='extras']/text()[contains(.,'Garage') or contains(.,'Parkeergelegenheid') ]/preceding::i[1][@class='fa fa-check']").get()
        if terrace:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)

        terrace = response.xpath("//td[@class='extras']/text()[contains(.,'  Lift')]/preceding::i[1][@class='fa fa-check']").get()
        if terrace:
            item_loader.add_value("elevator", True)
        else:
            item_loader.add_value("elevator", False)

        furnished = response.xpath("//td[@class='extras']/text()[contains(.,'Gemeubileerd')]/preceding::i[1][@class='fa fa-check']").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        postcode = response.xpath("//tr[td[.='Postcode:']]/td[2]/text()[normalize-space()]").get()
        item_loader.add_value("zipcode", postcode.strip())

        city = response.xpath("//tr[td[.='Adres:']]/td[2]/text()[normalize-space()]").get()
        item_loader.add_value("city", city.strip())

        item_loader.add_value("address", postcode.strip() + " " + city.strip())

        item_loader.add_value("landlord_phone", "040-2116149")
        item_loader.add_value("landlord_email", "info@brickvastgoed.nl")
        item_loader.add_value("landlord_name", "Brick Vastgoed")
        
        yield item_loader.load_item()

    
