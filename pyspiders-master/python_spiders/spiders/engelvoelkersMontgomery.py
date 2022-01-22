# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import *

class EngelvoelkerspavillondixellesSpider(scrapy.Spider):
    name = "engelvoelkersMontgomery"
    allowed_domains = ["engelvoelkers.com"]
    start_urls = (
        "https://www.engelvoelkers.com/en/search/?q=&startIndex=0&businessArea=residential&sortOrder=DESC&sortField=newestProfileCreationTimestamp&pageSize=18&facets=bsnssr%3Aresidential%3Bcntry%3Abelgium%3Btyp%3Arent%3Bobjcttyp%3Ahouse%3B",
        "https://www.engelvoelkers.com/en/search/?q=&startIndex=0&businessArea=residential&sortOrder=DESC&sortField=newestProfileCreationTimestamp&pageSize=18&facets=bsnssr%3Aresidential%3Bcntry%3Abelgium%3Btyp%3Arent%3Bobjcttyp%3Acondo%3B",
        #"https://www.engelvoelkers.com/fr/search/?q=&startIndex=16&businessArea=residential&sortOrder=DESC&sortField=sortPrice&pageSize=18&facets=bsnssr%3Aresidential%3Bcntry%3Abelgium%3Bobjcttyp%3Acondo%3Btyp%3Arent%3B",
        #"https://www.engelvoelkers.com/fr/search/?q=&startIndex=0&businessArea=residential&sortOrder=DESC&sortField=sortPrice&pageSize=18&facets=bsnssr%3Aresidential%3Bcntry%3Abelgium%3Btyp%3Arent%3Bobjcttyp%3Ahouse%3B",
        #"https://www.engelvoelkers.com/en/search/?q=&startIndex=0&businessArea=residential&sortOrder=DESC&sortField=sortPrice&pageSize=18&facets=bsnssr%3Aresidential%3Btyp%3Arent%3Bobjcttyp%3Ahouse%3B",
    )
    execution_type = "testing"
    country = "belgium" 
    locale = "fr" 
    thousand_separator = ","
    scale_separator = "."
    external_source="Engelvoelkersmontgomery_PySpider_belgium_fr"

    def start_requests(self):  
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for link in response.xpath("//a[contains(@class,'ev-property-container')]"):
            status = link.xpath(".//@href").get()
            if status and ("verhuurd" in status or "loue-" in status):
                print(status)
                continue
            item = {
                # "title": response.xpath(".//div[@class='ev-teaser-title']//text()").get(),
                # "room_count": response.xpath(".//div[img[@title='Bedrooms']]//span//text()").get(),
                # "bathroom_count": response.xpath(".//div[img[@title='Salles de bains']]//span//text()").get(),
                # "square_meters": response.xpath(".//div[img[@title='Surface Habitable']]//span//text()").get(),
                # "rent": response.xpath(".//div[@class='ev-teaser-price']/div[@class='ev-value']//text()").get(),
                "property_type": "house" if "house" in response.url else "apartment",
                # "currency": "EUR",
            } 

            yield scrapy.Request(
                response.urljoin(link.xpath(".//@href").get()),
                self.parse_detail,
                cb_kwargs=dict(item=item),
            )
        # next_page = response.xpath("//li/a[@class='ev-pager-next']/@href")
        yield from self.parse_next(response)

    def parse_next(self, response):
        """ parse next page"""
        xpath = ".//a[@class='ev-pager-next']"
        if response.xpath(xpath).get():
            for link in response.xpath(xpath):
                yield response.follow(link)

    def parse_detail(self, response, item):
        item_loader = ListingLoader(response=response)
        dontallow=response.xpath("//meta[@name='description']/@content").get()
        if dontallow and "verhuurd" in dontallow.lower():
            return 
        dontallow1=response.xpath("//h1[@class='ev-exposee-title ev-exposee-headline']/text()").get()
        if dontallow1 and "verhuurd" in dontallow1.lower():
            return 

        block =response.xpath("//h1[contains(.,'Dispositions relatives à la protection des données')]/text()").extract_first()
        if block:return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        title=response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title",title)
        room_count=response.xpath("//div[img[@title='Bedrooms']]//following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//div[img[@title='Bathrooms']]//following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        square_meters=response.xpath("//div[img[@title='Living area approx.']]//following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.strip().split(" ")[0])
        utilities=response.xpath("//span[contains(.,'Utilities')]/text()").get()
        if utilities:
            utilities=re.findall("\d+",utilities)
            item_loader.add_value("utilities",utilities[0])
        for k, v in item.items():
            item_loader.add_value(k, v)
        item_loader.add_xpath("description", ".//p[@itemprop='description']//text()")
        item_loader.add_xpath("images", ".//div[@id='keyVisual']//a/@href")
       
        # dontallowanothercity=response.url
        # if dontallowanothercity and "en-ca" in dontallowanothercity:
        #     return

        # dontallowverhuurd=response.xpath("//link[@rel='alternate']/@href").get()
        # if dontallowverhuurd and "rented" in dontallowverhuurd:
        #     return 
        dontallow=response.xpath("//h1//text()").get()
        if dontallow and "rented" in dontallow.lower().strip():
            return 

        phone = response.xpath("//span[@itemprop='telephone']//text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)

        item_loader.add_xpath("landlord_name", ".//div[@class='ev-exposee-contact-details-name']//text()")
        item_loader.add_value("landlord_email","pavillondixelles@engelvoelkers.com")
        item_loader.add_xpath("external_id", ".//input[@name='displayID']/@value")
        self.get_from_detail_panel(
            " ".join(response.xpath(".//li[@class='ev-exposee-detail-fact']//text()").getall()), item_loader
        )
        utilities = response.xpath("//ul[contains(@class,'ev-exposee-detail-facts')]/li[contains(.,'Charges')]/span/text()").get()
        if utilities:
            utilities = utilities.split(" ")[1].replace(",00","").strip()
            item_loader.add_value("utilities", utilities)
        address = response.xpath("substring-after(//div[contains(@class,'ev-exposee-subtitle')]/text(),'|')").get()
        if address:
            item_loader.add_value("address",address.split(",")[0].strip())
            item_loader.add_value("city", address.split(",")[-1].strip())
        furnished = response.xpath("//h1/text()[contains(.,'meublé ')]").get()
        if furnished:
            item_loader.add_value("furnished",True)

        rent=response.xpath("//div[@class='ev-key-fact-value']/span[@itemprop='price']/text()").get()
        if rent and rent != "0":
            item_loader.add_value("rent", rent.replace(",","").replace(".","").strip())
            item_loader.add_value("currency", "EUR")
        parking=response.xpath("//li[@class='ev-exposee-detail-fact']/label[contains(.,'Garage') or contains(.,'Parkings')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        # item_loader.add_xpath(
        #     "utilities", ".//span[@class='ev-exposee-detail-fact-value' and contains(.,'Charges')]//text()"
        # )
        
        yield item_loader.load_item()

    def get_from_detail_panel(self, text, item_loader):
        """check all keywords for existing"""
        keywords = {
            "parking": [
                "parking",
                "garage",
                "car",
                "aantal garage",
            ],
            "balcony": [
                "balcon",
                "nombre de balcon",
                "Nombre d",
                "balcony",
                "balcon arrière",
            ],
            "pets_allowed": ["animaux"],
            "furnished": ["meublé", "appartement meublé", "meublée"],
            "swimming_pool": ["piscine"],
            "dishwasher": ["lave-vaisselle"],
            "washing_machine": ["machine à laver", "lave linge"],
            "terrace": ["terrasse", "terrasse de repos", "terras"],
            "elevator": ["ascenseur", "ascenceur"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, True)