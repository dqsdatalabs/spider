# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import itemloaders
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import re
import dateparser


class LarockimmoSpider(scrapy.Spider):
    name = "larockimmo"
    allowed_domains = ["larockimmo.be"]
    start_urls = ("https://www.larockimmo.be/fr-BE/chercher-bien/a-louer",)
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
                # item["bathroom_count"] = node.xpath(
                #     ".//span[@class='estate-facts__item' and i[contains(@class,'fa-bath')]]//text()"
                # ).get()
                item["square_meters"] = node.xpath(
                    ".//span[@class='estate-facts__item' and i[contains(@class,'fa-arrows-alt')]]//text()",
                ).get()
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
        #item_loader.add_xpath("description", f"{main_block}//div[h2[.='Description']]/p//text()")
        item_loader.add_xpath("title", ".//span[@class='estate-detail-intro__block-text']/text()")
        item_loader.add_value(
            "zipcode", response.xpath(".//span[@class='estate-detail-intro__block-text'][2]/text()").get().split(" ")[0]
        )
        item_loader.add_xpath("images", ".//div[contains(@class,'owl-estate-photo')]/a/@href")
        #item_loader.add_xpath("landlord_phone", f"{main_block}//a[i[@class='fa fa-phone fa-2x']]/span[2]//text()")
        item_loader.add_xpath(
            "landlord_email",
            ".//div[@class='footer__agency']/p//a[contains(@href,'mailto')]/text()",
        )

        item_loader.add_value("landlord_phone", "+32 2 428 45 25")

        description = "".join(response.xpath("//div[@class='col-md-9']//p//text()").getall()).strip()
        if description:
            item_loader.add_value('description', description)
        
        utilities = re.search(r'[cC]harges.*?\d+€?', description)
        if utilities:
            item_loader.add_value("utilities", re.sub(r'\D', '', utilities.group(0)))

        if re.search(r'meubl[eé]+', description.lower()):
            item_loader.add_value('furnished', True)
        
        general = ''.join(response.xpath("//div[@class='col-md-9']/table[1]//text()").getall()) #contains general home information e.g.garage,balcony
        bathroom_count = re.search(r'salledebain\d', re.sub(r'\s', '',  general))
        if bathroom_count:
            item_loader.add_value('bathroom_count', bathroom_count[0][-1])

        available_date = re.search(r'[Dd]isponibilit[ée](\d+-\d+-\d+)', re.sub(r'\s', '',  general))
        if available_date:
            date_parsed = dateparser.parse(available_date.group(1), date_formats=["%d/%B/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        property_type = response.xpath("//th[contains(.,'Catégorie')]/following-sibling::td/text()").get()
        if property_type:
            if 'appartement' in property_type.lower() or 'flat' in property_type.lower() or 'triplex' in property_type.lower():
                item_loader.add_value("property_type", 'apartment')
            elif 'maison' in property_type.lower():
                item_loader.add_value("property_type", 'house')
            else:
                return

        self.get_from_detail_panel(
            " ".join(response.xpath(f"{table_xpath}//tr[td[contains(.,'Oui')]]/th//text()").getall()), item_loader
        )
        self.get_general(item_loader)
        item_loader.add_value("landlord_name", "Larock Immo")

        rent = response.xpath('//h1[@class="line-separator-after h2 estate-detail-intro__text"]/text()').getall()[-1].replace('€', '').strip()
        deposit = re.search(r'Garantie location \(nombre de mois\).\s+(\d)', ''.join(response.xpath("//div[@class='col-md-9']/table[2]//text()").getall()))
        if deposit and rent:
            item_loader.add_value('deposit', int(deposit.group(1)) * int(rent.replace('.', '')))

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
            #"furnished": ["meublé", "appartement meublé", "meublée"],
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