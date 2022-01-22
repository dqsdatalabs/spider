# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from scrapy import FormRequest, Request
from ..loaders import ListingLoader
from ..helper import *


class NamurtreviSpider(scrapy.Spider):
    name = "namurTrevi"
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        yield Request("https://www.trevi.be/fr/namur/residentiel/louer-bien-immobilier/", callback=self.jump)
    
    def jump(self, response):
        url = "https://www.trevi.be/Connections/request/xhr/infinite_projects.php"
        headers = {
            'Connection': 'keep-alive',
            'Accept': '*/*',
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept-Language': 'tr,en;q=0.9',
            'Cookie': '_gcl_au=1.1.221367090.1615818963; _ga=GA1.2.1815992789.1615818963; _fbp=fb.1.1615818962827.1809186404; _gid=GA1.2.1407053198.1616579594; PHPSESSID=9l773b9p3scihhndfmrtbj96ep; _gat_UA-114693467-1=1; _gat_UA-34968013-2=1; _gat_UA-36482596-1=1; _gat_UA-5575565-1=1; PHPSESSID=bv51763dndqvam351oe1i8g855'
        }
        payloads = [
            {
                "payload": "limit1=12&limit2=0&serie=0&filtre=filtre_cp&market=&lang=fr&type=2&goal=1&property-type=2&goal=1&search=1",
                "property_type": "apartment",
            },
            {
                "payload": "limit1=12&limit2=0&serie=0&filtre=filtre_cp&market=&lang=fr&type=1&goal=1&property-type=1&goal=1&search=1",
                "property_type": "house",
            },
            {
                "payload": "limit1=12&limit2=0&serie=0&filtre=filtre_cp&market=&lang=fr&type=34&goal=1&property-type=34&goal=1&search=1",
                "property_type": "apartment",
            },
        ]
        for item in payloads:
            yield Request(url,
                        method="POST",
                        headers=headers,
                        body=item["payload"],
                        dont_filter=True,
                        callback=self.parse, 
                        meta={"property_type": item["property_type"], "url": url, "headers": headers, "payload": item["payload"]})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 1)
        seen = False

        for item in response.xpath("//a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.parse_detail, meta={"property_type": response.meta["property_type"]})
        
        if page == 1 or seen:
            url = response.meta["url"]
            headers = response.meta["headers"]
            payload = response.meta["payload"].replace("&limit2=" + str((page * 12) - 12), "&limit2=" + str(page * 12)).replace("&serie=" + str(page - 1), "&serie=" + str(page))

            yield Request(url,
                    method="POST",
                    headers=headers,
                    body=payload,
                    dont_filter=True,
                    callback=self.parse, 
                    meta={"property_type": response.meta["property_type"], "url": url, "headers": headers, "payload": payload, "page": page + 1})

    def parse_detail(self, response):
        property_type = response.meta["property_type"]
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        
        prop_type = "".join(response.xpath("//div[@class='bien__content']//h2/text()").getall())
        if "furnished" in property_type:
            item_loader.add_value("furnished", True)
            property_type = property_type.replace("furnished", "").strip()
        if property_type:
            item_loader.add_value("property_type", property_type)
        elif "Maison" in prop_type or "Villa" in prop_type:
            item_loader.add_value("property_type", "house")
        elif "Appartement" in prop_type:
            item_loader.add_value("property_type", "apartment")
        elif "studio" in prop_type.lower():
            item_loader.add_value("property_type", "studio")
        else: return
        
        zip_code = response.xpath("//table[@class='table table-striped']//tr[td[contains(.,'Code postal')]]/td[2]//text()").get()
        if zip_code:
            temp = zip_code.split("-")
            item_loader.add_value("zipcode", temp[0].strip())
            if len(temp) >= 2:
                item_loader.add_value("city", "-".join([x.strip() for x in temp[1:]]))
                
        address = response.xpath("//tr/td[contains(.,'Adresse')]/following-sibling::td/text()").get()
        if address:
            item_loader.add_value("address", address)
        elif len(zip_code.split("-")) >= 2:
            item_loader.add_value("address", zip_code.split("-")[1])
            
        
        main_block = response.xpath(".//div[@class='bien--details']")

        if len(main_block) == 1:
            detail_node = main_block.xpath(".//div[@class='bien__content']/div")
            # print(detail_node.xpath(".//table").get())
            item_loader.add_value("external_id", detail_node.xpath("./div/p[1]/b/text()").get())
            item_loader.add_value("title", main_block.xpath("//meta[@property='og:title']/@content").get())

            item_loader.add_value("description", main_block.xpath("//p[2]//text()").get())

            item_loader.add_xpath("images", ".//div[@class='bien--details']//a[@data-fancybox='gallery']/@href")

            rent = response.xpath("//td[contains(.,'Loyer')]/following-sibling::td/text()").get()
            if rent:
                price = rent.split("€")[0].strip()
                item_loader.add_value("rent", price)
                item_loader.add_value("currency", "EUR")
            
            self.get_general(item_loader, response)
            item_loader.add_xpath("landlord_phone", ".//a[contains(@href,'tel:')]//text()")
            item_loader.add_value("landlord_name", "Trevi-Namur")
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
        
        furnished = response.xpath(
            f".//table[@class='table table-striped']//tr[td[contains(.,'Meublé')]]/td[2]//text()"
        ).get()
        if furnished and furnished.casefold() == "oui":
            item_loader.add_value("furnished", True)

    def get_lang(self):
        return {
            "Accept-Language": self.locale,
        }