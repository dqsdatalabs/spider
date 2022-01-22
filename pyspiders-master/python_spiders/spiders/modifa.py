# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import *
import dateparser


class ModifaSpider(scrapy.Spider):
    name = "modifa"
    allowed_domains = ["modifa.be"]
    start_urls = ("https://www.modifa.be/fr-BE/chercher-bien/a-louer",)
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        url = response.xpath(".//div[@class='infinite-scroll']/a/@href").get()
        pages = int(self.sub_string_between(url, "MaxPage=", "&"))
        for i in range(pages + 1):
            yield response.follow(
                url.replace("CurrentPage=0", f"CurrentPage={i}"),
                self.parse_list,
            )

    def parse_list(self, response):
        for node in response.xpath(".//div[@class='estate-list__item']"):
            link = node.xpath("./a/@href")
            if len(link) > 0 and ("/appartement/" in link.get() or "/maison/" in link.get()):
                item = {}
                item["room_count"] = node.xpath(
                    ".//span[@class='estate-facts__item' and i[contains(@class,'fa-bed')]]//text()"
                ).get()
                item["bathroom_count"] = node.xpath(
                    ".//span[@class='estate-facts__item' and i[contains(@class,'fa-bath')]]//text()"
                ).get()
                item["square_meters"] = node.xpath(
                    ".//span[@class='estate-facts__item' and i[contains(@class,'fa-arrows-alt')]]//text()",
                ).get()
                item["property_type"] = "apartment" if "Appartement" in response.url else "house"
                item["city"] = node.xpath(".//div[@class='estate-card__text']//text()").get().strip()
                item["rent_string"] = node.xpath(".//div[@class='estate-card__text-details']//text()[2]").get()
                if node.xpath(".//span[@class='estate-facts__item' and i[contains(@class,'fa-car')]]//text()").get():
                    item["parking"] = True
                yield response.follow(
                    link.get(),
                    self.parse_detail,
                    cb_kwargs=dict(item=item),
                )

    def parse_detail(self, response, item):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        for k, v in item.items():
            item_loader.add_value(k, v)
        main_block = ".//section[@class='section']"
        table_xpath = ".//*[@class='estate-table']"
        item_loader.add_xpath("description", f"{main_block}//div[h2[.='Description']]/p//text()")

        utilities = response.xpath("substring-after(//p/text/text()[contains(.,'Charges') and contains(.,'€')],'Charges: ')").get()
        if utilities:
            item_loader.add_value("utilities", int(float(utilities.split('€')[0].strip())))

        title = "".join(response.xpath("//span[@class='estate-detail-intro__block-text']/text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h3[contains(.,'Adresse')]/following-sibling::p/text()").get()
        addr = response.xpath("//span[@class='estate-detail-intro__block-text'][2]/text()").get()
        if address:
            item_loader.add_value("address", address)
        elif addr:
            item_loader.add_value("address", addr.strip())
            
        item_loader.add_value(
            "zipcode", response.xpath(".//span[@class='estate-detail-intro__block-text'][2]/text()").get().split(" ")[0]
        )
        item_loader.add_xpath("images", ".//div[contains(@class,'owl-estate-photo')]/a/@href")
        phone = response.xpath(f"{main_block}//a[i[@class='fa fa-phone fa-2x']]/span[2]//text()").get()
        item_loader.add_xpath("landlord_phone", f"{main_block}//a[i[@class='fa fa-phone fa-2x']]/span[2]//text()")
        item_loader.add_xpath(
            "landlord_email",
            f".//div[@class='footer__agency']/p[.//a[contains(.,'{phone}')]]//a[contains(@href,'mailto')]/text()",
        )
        dt = response.xpath(
            ".//*[contains(@class,'estate-table')]//tr[th[contains(.,'Disponibilité')]]/td//text()"
        ).get()
        if dt:
            dt = dateparser.parse(dt)
            if dt:
                item_loader.add_value(
                    "available_date",
                    dt.date().strftime("%Y-%m-%d"),
                )
        self.get_from_detail_panel(
            " ".join(response.xpath(f"{table_xpath}//tr[td[contains(.,'Oui')]]/th//text()").getall()), item_loader
        )
        self.get_general(item_loader)
        item_loader.add_xpath(
            "landlord_name", f".//div[@class='footer__agency']/p[.//a[contains(.,'{phone}')]]/text()[1]"
        )
        yield item_loader.load_item()

    def get_general(self, item_loader):
        keywords = {
            "external_id": "Référence",
            "floor": "Étages",
            "utilities": "Charges",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(k, f".//*[@class='estate-table']//tr[th[contains(.,'{v}')]]/td//text()")

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
            "elevator": ["ascenseur", "elevator"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, True)

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, True)

    def sub_string_between(self, source, s1, s2):
        tmp = source[source.index(s1) + len(s1) :]
        return tmp[: tmp.index(s2)]