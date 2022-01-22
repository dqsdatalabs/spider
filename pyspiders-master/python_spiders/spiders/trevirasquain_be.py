# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re


class MySpider(Spider):
    name = "trevirasquain_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    external_source = "Trevirasquain_PySpider_belgium_fr"
    def start_requests(self):
        start_urls = [
            {"url": "https://www.benoitrasquain.be/fr/residentiel/louer-bien-immobilier/appartement", "property_type": "apartment", "type":"2"},
            {"url": "https://www.benoitrasquain.be/fr/residentiel/louer-bien-immobilier/maison", "property_type": "house", "type":"1"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.jump,
                            #dont_filter = True,
                            meta={'property_type': url.get('property_type'),
                            "type":url.get('type')})

    def jump(self, response):

        seen = False
        for card in response.xpath('//a[@class="card bien"]/@href').extract():
            url = response.urljoin(card)
                    
            yield response.follow(url, self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True

        headers = {
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            # 'Cookie': 'PHPSESSID=t5ocns8esq2apg2l1gner2cslf; PHPSESSID=oe4rjpqpt1m249qanqtgm1e8rh',
        }
        page = response.meta.get("page", 1)
        data = {
            "limit1": "12",
            "limit2": str(page * 12),
            "serie": str(page),
            "filtre": "filtre_cp",
            "market": "",
            "lang": "fr",
            "type": f"{response.meta.get('type')}",
            "goal": "1",
            "property-type":f"{response.meta.get('type')}",
            "goal": "1",
            "search": "1",
        }
        if seen:
            yield FormRequest(
                "https://www.benoitrasquain.be/Connections/request/xhr/infinite_projects.php",
                # body=json.dumps(data),
                headers=headers,
                formdata=data,
                dont_filter=True,
                callback=self.jump,
                meta={"page": page + 1, "type": response.meta.get('type'),
                "property_type": response.meta.get('property_type')},
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Trevirasquain_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        s_meter = response.xpath("//tr[contains(.,'Superficie habitable')or contains( .,'Superficie' )]/td[2]/text()[ . !='0 m²']").extract_first()
        
        bathroom_count = response.xpath("//tr[contains(.,'bain') or contains(.,'douche')]/td[2]/text()[ .!='0']").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_xpath("external_id", "//p[contains(.,'Référence :')]/b/text()")
        
        desc = "".join(
            response.xpath("//div[@class='bien__content']//p[2]/text()").extract()
        )
        if desc:
            description = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", description.lstrip().rstrip())
            if "terrasse" in desc:
                item_loader.add_value("terrace", True)
        
        price = response.xpath(
            "//tr[contains(.,'Loyer / mois')]/td[2]/text()[contains(., '€')]"
        ).extract_first()
        if price:
            item_loader.add_value("rent_string", price)
            # item_loader.add_value("currency", "EUR")
            
        address = "".join(
            response.xpath("//tr[contains(.,'Adress')]/td[2]/text()").extract()
        ).strip()
        if address==",":
            zip_code = response.xpath(
                "//tr[contains(.,'Code postal')]/td[2]/text()"
            ).extract_first()
            item_loader.add_value("address", zip_code)
            item_loader.add_value("zipcode", zip_code.split("-")[0].strip())
            item_loader.add_value("city", zip_code.split("-")[1].strip())
        elif address:

            item_loader.add_value("address", address)
            zip_code = response.xpath(
                "//tr[contains(.,'Code postal')]/td[2]/text()"
            ).extract_first()
            if zip_code:
                item_loader.add_value("zipcode", zip_code.split("-")[0].strip())
                item_loader.add_value("city", zip_code.split("-")[1].strip())
        else:
                address2 = response.xpath(
                    "//tr[contains(.,'Code postal')]/td[2]/text()"
                ).extract_first()
                if address2:
                    item_loader.add_value("address", address2)
                    item_loader.add_value("zipcode", address2.split("-")[0].strip())
                    item_loader.add_value("city", address2.split("-")[1].strip())
        
        property_type = response.xpath(
            "//tr[contains(.,'Type de bien')]/td[2]/text()"
        ).extract_first()
        if property_type:
            if "studio" in property_type.lower():
                item_loader.add_value("property_type", "studio")
            elif "appartement" in property_type.lower() or "duplex" in property_type.lower() or "étage" in property_type.lower() or "rez-de-chaussée" in property_type.lower() or "penthouse" in property_type.lower():
                item_loader.add_value("property_type", "apartment")
            elif "maison" in property_type.lower() or "villa" in property_type.lower():
                item_loader.add_value("property_type", "house")
            else:
                item_loader.add_value("property_type", response.meta.get('property_type'))
        elif not property_type:
            return
        room = "".join(response.xpath("//tr[contains(.,'Nbre de chambres')]/td[2]/text()[ .!='0']").extract())
        if room:    
            item_loader.add_value("room_count", room)
        elif "studio" in property_type or "studio" in desc:
            item_loader.add_value("room_count", "1")
        
            
    
        s_meter = response.xpath(
            "//tr[contains(.,'Superficie habitable')]/td[2]/text()"
        ).extract_first()
        if  s_meter and s_meter!="0 m²":
            item_loader.add_value("square_meters", s_meter.replace("m²", ""))

        floor = response.xpath(
            "//tr[contains(.,'Etage')]/td[2]/text()"
        ).extract_first()
        if floor:
            item_loader.add_value("floor", floor)

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='slide']//div[2]//div[@class='col-6']/a/@href"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)

        phone = response.xpath(
            '//div[contains(@class,"tel")]/span/a/@href'
        ).get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("tel:", ""))
        item_loader.add_value("landlord_name", "Trevi Orta")
        item_loader.add_value("landlord_email", "info@treviorta.be")
        furnish = response.xpath(
            "//table[@class='table table-striped']//tr[./td[.='Meublé']]/td[2]/text()"
        ).get()
        if furnish:
            if "Oui" in furnish:
                item_loader.add_value("furnished", True)
            elif "Yes" in furnish:
                item_loader.add_value("furnished", True)
            elif "No" in furnish:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", False)

        parking = response.xpath(
            "//table[@class='table table-striped']//tr[./td[.='Parking(s)' or .='Garage(s)' ]]/td[2]/text()"
        ).get()
        if parking:
            item_loader.add_value("parking", True)

        utilities = response.xpath(
            "//table[@class='table table-striped']//tr[./td[.='Charges / mois']]/td[2]/text()"
        ).get()
        utilities_2 = response.xpath("//div[@class='d-print-none']/p[(contains(.,'Charge') or contains(.,'charge')) and contains(.,'€') ]//text()").get()

        if utilities:
            utilities = utilities.split("€")[0]
            item_loader.add_value("utilities", utilities)
        elif not utilities and utilities_2:
            numbers = re.findall(r'\d+(?:\.\d+)?', utilities_2)
            if numbers:
                item_loader.add_value("utilities", numbers[0])
            
        yield item_loader.load_item()
