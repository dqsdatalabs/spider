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
    name = 'interhouse_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    
    data = {
        "action": "building_results_action",
        "query": ''
        }

    def start_requests(self):

        start_urls = [
            {
                "query" : "?location_id=Haarlem_Algemeen&number_of_results=100&sort=date-desc&display=list&language=nl_NL"
            },
            {
                "query" : "?location_id=Hilversum_Algemeen&number_of_results=50&sort=date-desc&display=list&language=nl_NL"
            },
            {
                "query" : "?location_id=Rotterdam_Algemeen&number_of_results=50&sort=date-desc&display=list&language=nl_NL"
            },
            {
                "query" : "?location_id=Sassenheim_Algemeen&number_of_results=50&sort=date-desc&display=list&language=nl_NL"
            },
        ] 
        
        headers = {
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            "origin": "https://interhouse.nl"
        }
       
        url = "https://interhouse.nl/wp-admin/admin-ajax.php"
        for data in start_urls:
            self.data['query'] = data.get('query')
            yield FormRequest(
                url,
                formdata=self.data,
                headers=headers,
                callback=self.parse,
            )
    
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//ul[contains(@class,'grid-1 list-unstyled')]/li"):
            follow_url = item.xpath(".//a[contains(@class,'c-button')]/@href").get()
            prop_type = item.xpath(".//p[.='Type woning:']/following-sibling::p/text()").get()
            #{'Appartement': 'OK', 'Woonhuis': 'OK', 'Garage': 'OK', 'Studio': 'OK', 'Villa': 'OK'}
            if prop_type and "Appartement" in prop_type:
                prop_type = "apartment"
            elif prop_type and "Studio" in prop_type:
                prop_type = "studio"
            elif prop_type and ("Villa" or "Woonhuis") in prop_type:
                prop_type = "house"
            else:
                prop_type = None
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":prop_type})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Interhouse_PySpider_" + self.country + "_" + self.locale)

        rented = response.xpath("//table[@class='building-data-table']//tr[./td[.='Status']]/td[2]//text()").get()
        if "Verhuurd" in rented:
            return
        title = " ".join(response.xpath("//h2[@class='entry-title']//text()").extract())
        item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)

        prop_type = response.meta.get("property_type")
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return

        desc = "".join(response.xpath("//div[@class='building-description']/text()").extract())
        item_loader.add_value("description", desc.strip())
        item_loader.add_xpath("bathroom_count", "//tr[td[.='Badkamers']]/td[2]/text()")

        price = response.xpath("//table[@class='building-data-table']//tr[./td[.='Huurprijs']]/td[2]/text()").get()
        if price:
            item_loader.add_value(
                "rent", price.split("€")[1].strip().split(" ")[0])
            item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//table[@class='building-data-table']//tr[./td[.='Waarborgsom']]/td[2]//text()").get()
        if deposit:
            item_loader.add_value(
                "deposit", deposit.split("€")[1])

        item_loader.add_xpath(
            "external_id", "//table[@class='building-data-table']//tr[./td[.='Object ID']]/td[2]/text()"
        )

        square = response.xpath(
            "//table[@class='building-data-table']//tr[./td[.='Oppervlakte']]/td[2]/text()"
        ).get()
        if square:
            item_loader.add_value(
                "square_meters", square.split("m")[0]
            )
        room_count = response.xpath(
            "//table[@class='building-data-table']//tr[./td[.='Slaapkamers' or .='Kamers']]/td[2]/text()"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        address = response.xpath("//div[@class='entry-title__address']/text()").get()
        if address:    
            item_loader.add_value("address", address.strip())
            zipcode = ""
            for i in address.split(" "):
                if i.isdigit():
                    zipcode = i
                    break
            if len(zipcode) == 4:
                item_loader.add_value("zipcode", zipcode)

        city = response.xpath("//div[@class='entry-title__city']/text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        available_date = response.xpath(
            "//table[@class='building-data-table']//tr[./td[.='Beschikbaarheidsdatum']]/td[2]/text()[.!='Per direct']").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

       
        furnished = response.xpath("//table[@class='building-data-table']//tr[./td[.='Interieur']]/td[2]/text()").get()
        if furnished:
            if "Gestoffeerd" in furnished:
                item_loader.add_value("furnished", False)
            elif "Gemeubileerd" in furnished:
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        
        parking = response.xpath(
            "//table[@class='building-data-table']//tr[./td[.='Parkeergelegenheid']]/td[2]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        

        elevator = response.xpath(
            "//table[@class='building-data-table']//tr[./td[.='Lift']]/td[2]/text()").get()
        if elevator:
            if "Ja" in elevator:
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        
        balcony = response.xpath(
            "//table[@class='building-data-table']//tr[./td[.='Balkon']]/td[2]/text()").get()
        if balcony:
            if "Ja" in balcony:
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)

        pet = response.xpath("//div[@class='building-description']/text()[contains(.,'Geen huisdieren')]").get()
        pet2 = response.xpath("//div[@class='building-description']/text()[contains(. ,'Huisdieren')]").get()
        if pet:
            item_loader.add_value("pets_allowed", False)
        elif pet2:
            item_loader.add_value("pets_allowed", True)

        terrace = response.xpath("//tr[td[.='Dakterras']]/td[2]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        item_loader.add_xpath("energy_label", "//table[@class='building-data-table']//tr[./td[.='Energielabel']]/td[2]//text()")

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//a[@class='building-gallery__image-link']/@href"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        
        
        item_loader.add_value("landlord_phone", "+310208450527")
        item_loader.add_value("landlord_name", "Interhouse Verhuurmakelaars")
        item_loader.add_value("landlord_email", "haarlem.vh@interhouse.nl")

        yield item_loader.load_item()