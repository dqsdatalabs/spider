# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from scrapy import FormRequest, Request
from ..loaders import ListingLoader
from ..helper import * 
  
 
class VenturiSpider(scrapy.Spider):
    name = "venturi" 
    allowed_domains = ["venturi.be"]
    start_urls = [
        "https://www.venturi.be/fr/residentiel/louer-bien-immobilier/maison",
    ]
    follow_urls = [
        "https://www.venturi.be/fr/residentiel/louer-bien-immobilier/appartement",
        "https://www.venturi.be/fr/residentiel/louer-bien-immobilier/flat",
    ]
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    index = 0
    base_url = "https://www.trevi.be/Connections/request/xhr/infinite_projects.php"

    def start_request(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                headers=self.get_lang(),
            )

    def parse(self, response, **kwargs):

        """ post and infinite scroll one by one"""
        if "/louer-bien-immobilier/maison" in response.url:
            yield FormRequest(
                url=self.base_url,
                formdata={
                    "limit1": "12",
                    "filtre": "filtre_cp",
                    "market": "",
                    "lang": "fr",
                    "type": "1",
                    "goal": "1",
                    "property-type": "1",
                    "goal": "1",
                    "search": "1",
                },
                callback=self.after_post,
                headers=self.get_lang(),
                meta={"prop":"house"},
                cb_kwargs=dict(property_type="house"),
            )

        for s in self.follow_urls:
            if "appartement" in s:
                yield FormRequest(
                    url=self.base_url,
                    formdata={
                        "limit1": "12",
                        "filtre": "filtre_cp",
                        "market": "",
                        "lang": "fr",
                        "id_projects": "",
                        "type": "2",
                        "goal": "1",
                        "property-type": "2",
                        "goal": "1",
                        "search": "1",
                    },
                    dont_filter=True,
                    callback=self.after_post,
                    meta={"property_type":"apartment"},
                    headers=self.get_lang(),
                    cb_kwargs=dict(property_type="apartment"),
                )

            if "flat" in s:
                yield FormRequest( 
                    url=self.base_url,
                    formdata={
                        "limit1": "12",
                        "filtre": "filtre_cp",
                        "market": "",
                        "lang": "fr",
                        "type": "34",
                        "goal": "1",
                        "property-type": "34",
                        "goal": "1",
                        "search": "1",
                    },
                    dont_filter=True,
                    callback=self.after_post,
                    meta={"property_type":"apartment"},
                    headers=self.get_lang(),
                    cb_kwargs=dict(property_type="apartment"),
                )
        yield from self.after_post(response, "")

    def after_post(self, response, property_type):
        
        """ yield detail page and go next"""
        if len(response.text) > 0:
            body = str(response.request.body)
            if "limit1" in body:
                all_text = body.split("&")
                tmp = all_text[0].split("=")
                start = 12
                tmp = all_text[1].split("=")
                end = int(tmp[1]) + 12
                ser = str(int(all_text[2].split("=")[1]) + 1)
                prop_type = all_text[9].split("=")[1]
                yield FormRequest(
                    url=self.base_url,
                    formdata={
                        "limit1": str(start),
                        "limit2": str(end),
                        "serie": ser,
                        "filtre": "filtre_cp",
                        "market": "",
                        "lang": "fr",
                        "type": prop_type,
                        "goal": "1",
                        "property-type": prop_type,
                        "search": "1",
                    },
                    dont_filter=True,
                    headers=response.request.headers,
                    callback=self.after_post,
                    cb_kwargs=dict(property_type=property_type),
                )

            xpath = "//div[@class='row liste_biens']/div/a"
            for link in response.xpath(xpath):
                url = response.urljoin(link.xpath("./@href").extract_first())
                yield response.follow(
                    url,
                    self.parse_detail,
                    dont_filter=True,
                    cb_kwargs=dict(property_type=property_type),
                )

        
        
            self.index += 1

    def parse_detail(self, response, property_type):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        if "furnished" in property_type:
            item_loader.add_value("furnished", True)
            property_type = property_type.replace("furnished", "").strip()
        dontallow=response.url
        if dontallow and "9023" in dontallow:
            return 

        if property_type:
            item_loader.add_value("property_type", property_type)
        else: 
            property_type = response.xpath("//td[contains(.,'Type')]/following-sibling::td/text()").get()
            if property_type:
                if 'maison' in property_type.lower() or "villa" in property_type.lower():
                    item_loader.add_value("property_type", 'house')
                elif 'woning' in property_type.lower():
                    item_loader.add_value("property_type", 'house')
        propertycheck=item_loader.get_output_value("property_type")
        if not propertycheck:
            propertycheck=response.xpath("//meta[@property='og:title']/@content").get()
            if propertycheck and "bel-etage" in propertycheck:
                item_loader.add_value("property_type","aparment")
            elif propertycheck and "woning" in propertycheck:
                item_loader.add_value("property_type","house")                

        address = response.xpath("//b[contains(.,'Adresse')]/../../../td[last()]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())

        utilities = " ".join(response.xpath("//span[contains(.,'Charges mensuelles')]/..//text()").getall()).strip()
        if utilities:
            item_loader.add_value("utilities", utilities.split(':')[-1].split(',')[0].strip().replace(' ', ''))

        main_block = response.xpath(".//div[@class='bien--details']")

        if len(main_block) == 1:
            detail_node = main_block.xpath(".//div[@class='bien__content']/div")
            item_loader.add_value("external_id", detail_node.xpath("./div/p[1]/b/text()").get())
            item_loader.add_value("title", main_block.xpath("//meta[@property='og:title']/@content").get())

            description = " ".join(response.xpath("//p[contains(.,'Référence')]/following-sibling::p//text()").getall()).strip()   
            if description:
                item_loader.add_value("description", description.replace('\xa0', ''))
            
           
            item_loader.add_xpath("images", ".//div[@class='bien--details']//a[@data-fancybox='gallery']/@href")
         
            rent = response.xpath("//td[contains(.,'Loyer')]/following-sibling::td[1]/text()").get()
            if rent:
                rent = rent.split('€')[0].strip().split(',')[0].replace('.', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))
                item_loader.add_value("currency", 'EUR')

            self.get_general(item_loader, response)
            item_loader.add_xpath("landlord_phone", ".//a[contains(@href,'tel:')]//text()")
            item_loader.add_value("landlord_name", "TREVI Venturi")
            yield item_loader.load_item()

    def get_general(self, item_loader, response):
        keywords = {
            "square_meters": "Superficie habitable",
            "room_count": "Nbre de chambres", 
            "bathroom_count": "Salle(s) de bain(s)",
            "floor": "Etage",
            "utilities": "Charges / mois",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(k, f".//table[@class='table table-striped']//tr[td[contains(.,'{v}')]]/td[2]//text()")
        parking = response.xpath(
            f".//table[@class='table table-striped']//tr[td[contains(.,'Garage(s)')]]/td[2]//text()"
        ).get()
        if parking and parking != "0":
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath(
                f".//table[@class='table table-striped']//tr[td[contains(.,'Parking(s)')]]/td[2]//text()"
            ).get()
            if parking and parking != "0":
                item_loader.add_value("parking", True)
        zip_code = response.xpath(
            f".//table[@class='table table-striped']//tr[td[contains(.,'Code postal')]]/td[2]//text()"
        ).get()
        if zip_code:
            temp = zip_code.split("-")
            item_loader.add_value("zipcode", temp[0].strip())
            if len(temp) >= 2:
                item_loader.add_value("city", "-".join([x.strip() for x in temp[1:]]))
        furnished = response.xpath(
            f".//table[@class='table table-striped']//tr[td[contains(.,'Meublé')]]/td[2]//text()"
        ).get()
        if furnished and furnished.casefold() == "oui":
            item_loader.add_value("furnished", True)

    def get_lang(self):
        return {
            "Accept-Language": self.locale,
        }