# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import dateparser
import re


class BureaunelisSpider(scrapy.Spider):
    name = "bureaunelis"
    allowed_domains = ["bureaunelis.com"]

    start_urls = [
        {
            "url" : [
                "https://www.bureaunelis.com/Rechercher/Appartement%20Locations%20/Locations/Type-03%7CAppartement/Localisation-/Prix-/Tri-PRIX%20DESC,COMM%20ASC,CODE",
            ],
            "property_type" : "apartment",
        },
        { 
            "url" : [
                "https://www.bureaunelis.com/Rechercher/Maison%20Locations%20/Locations/Type-01%7CMaison/Localisation-/Prix-/Tri-PRIX%20DESC,COMM%20ASC,CODE"
            ],
            "property_type" : "house"
        },
    ]
        
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = "," 

    def start_requests(self):
        for url in self.start_urls:
            for item in url.get("url"):
                yield  scrapy.Request(item,
                            callback=self.parse,
                            headers=self.get_lang(),
                            meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        for link in response.xpath("//a[contains(@class,'zoom-cont2')]/@href"):
            if "javascript" not in link.get():
                yield response.follow(
                    link,
                    self.parse_detail,
                    headers=self.get_lang(),
                    meta={"property_type": response.meta.get("property_type")},
                )

    def parse_detail(self, response):
        stats = []
        for temp in response.xpath(".//div[@class='row']//div[@class='row']/div"):
            stats.append(temp)
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        item_loader.add_xpath("description", ".//div[div/div[@id='carousel']]//p/text()")
        item_loader.add_xpath("title", ".//h1[@class='liste-title']/text()")
        # dt = response.xpath('//td[contains(.,"Availability")]/following-sibling::td/text()').get()
        # if dt:
        #     date_parsed = dateparser.parse(dt, date_formats=["%m-%d-%Y"], languages=["fr"])
        #     if date_parsed:
        #         date = date_parsed.strftime("%Y-%m-%d")
        #         item_loader.add_value("available_date", date)
        item_loader.add_xpath( 
            "rent_string", ".//table[@class='table table-striped']//tr[td//text()[contains(.,'Prix')]]/td[2]//text()"
        )

        item_loader.add_xpath("external_id", ".//div[@class='ref-tag']//b//text()")
        item_loader.add_xpath("images", ".//div[@id='carousel']//div/a/@href")
        item_loader.add_value("landlord_phone", "04 / 342.83.18")
        item_loader.add_value("landlord_email", "info@bureaunelis.com")
        item_loader.add_value("landlord_name", "Bureau Nélis")
        broom=item_loader.get_output_value("bathroom_count")

        if not broom:
            bathroomcount1=response.xpath("//div[@class='col-xs-4']/i[@class='fas fa-bath']/following-sibling::text()").get()
            if bathroomcount1:
                bath=re.findall("\d+",bathroomcount1)
                item_loader.add_value("bathroom_count",bath)
        
        adres=response.xpath("//span[@class='glyphicon glyphicon-map-marker']/following-sibling::text()").get()
        if adres:
            adres=adres.replace("\xa0","")
            item_loader.add_value("address",adres)
            city=adres.split(" ")[-1]
            item_loader.add_value("city",city)
            zadres=adres.split("-")[-1]
            zipcode=re.findall("\d+",zadres)
            item_loader.add_value("zipcode",zipcode)

        deposit = response.xpath('//td[contains(.,"Garantie locative")]/following-sibling::td/text()').get()
        if deposit:
            d=re.findall("\d+",deposit) 
            item_loader.add_value("deposit",d[0])
        
        availabledate=response.xpath('//td[contains(.,"Disponibilité")]/following-sibling::td/text()').get()
        if availabledate:
            item_loader.add_value("available_date", availabledate)

        self.get_general(stats, item_loader)
        self.get_from_detail_panel(
            " ".join(
                response.xpath(
                    f".//table[@class='table table-striped']//tr[td[2][not(contains(.,'Non'))]]/td[1]/text()"
                ).getall()
            ),
            item_loader,
        )
        yield item_loader.load_item()

    def get_general(self, stats, item_loader):
        keywords = {
            "address": "Adresse",
            "square_meters": "Superficie séjour",
            "floor": "Composition des surfaces",
            "utilities": "Charges",
            "room_count": "Chambres",
            "bathroom_count": "Salles de bain",
        }
        for k, v in keywords.items():
            for temp in stats: 
                if temp.xpath(
                    f".//table[@class='table table-striped']//tr[td[1]/text()[contains(.,'{v}')]]/td[2]/text()"
                ):
                    item_loader.add_value(
                        k,
                        temp.xpath(
                            f".//table[@class='table table-striped']//tr[td[1]/text()[contains(.,'{v}')]]/td[2]/text()"
                        ).get(),
                    )

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


    def get_lang(self):
        return {
            "Accept-Language": self.locale,
        }
