# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class HouseloftComSpider(scrapy.Spider):
    name = 'houseloft_com'
    allowed_domains = ['houseloft.com']
    start_urls = ['https://www.houseloft.com/ricerca-immobili/affitto/localita/immobili.html?categoria=1%2C10%2C8%2C9%2C14&stato=1&tipologia=Affitto%7C0&provincia=0&bagnida=0&regione=0&camereda=0&localita=0&vani=0&languageselectmot=it&comune=0&mqda=&mqfinoa=&prezzo_a=0&offset=0']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        next_pages = response.css(
            "ul.page-pagination.section-space--top--30 > li > a::attr(href)").extract()
        next_pages[0] = self.start_urls[0]
        for i in range(len(next_pages)):
            yield Request(next_pages[i], callback=self.page_follower, dont_filter=True)

    # 1. FOLLOWING
    def page_follower(self, response):
        for appartment in response.css("article.property-listing-simple-2.section-space--bottom--40"):
            url = appartment.css(
                "div>div>a").attrib['href']
            address = appartment.css("span.location::text").get()
            yield Request(url,
                          callback=self.populate_item,
                          dont_filter=True,
                          meta={
                              "address": address
                          })

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_details = response.css(
            "div.property-info-agent.clear>span::text").extract()

        square_meters = property_details[3].strip().split(" ")[0]
        rooms = property_details[4].strip().split(" ")[0]
        bathrooms = property_details[6].strip().split(" ")[0]

        title = response.css(
            'h2.entry-title.single-property-title::text').get()

        external_id = response.css(
            'div.riferimentoimm::text').get().split("Rif. ")[1]

        try:
            rent = response.css(
                '#rev_slider_34_1_wrapper > div.property-page-price>meta::attr(content)').get().split("€")[1].strip()

            if "." in rent:
                rent_array = rent.split(".")
                rent = rent_array[0] + rent_array[1]

            if ',' in rent:
                rent = rent.split(',')[0]
            rent = int(rent)
        except:
            return

        description_elements = response.css(
            "p.descrizione::text").extract()
        description = ''

        for item in description_elements:
            if "e-mail:" not in item and "tel." not in item:
                description += item

        property_type = response.css(
            "body > div.page-wrapper.section-space--inner--bottom--50.section-space--inner--topnegative--80.container-property-single > div > div > div > div > div.row > div.col-lg-9.col-12 > div:nth-child(10) > div:nth-child(1) > a::text").get()

        try:
            if "appartamento" in property_type.lower():
                property_type = 'apartment'
            if "loft" in property_type.lower():
                property_type = "studio"
            if "attico" in property_type.lower() or "mansarda" in property_type.lower():
                property_type = "room"
        except:
            return

        floor_plan_images = response.css(
            'a.single-gallery-thumb>img.img-fluid::attr(src)').extract()

        floor_plan = []
        for image in floor_plan_images:
            floor_plan.append("https://www.houseloft.com/" + image)

        images = response.css('img.img-responsive::attr(src)').extract()

        features = response.css(
            "div.col-md-6.col-sm-6.dettimmcar")

        furnished = None
        city = None
        deposit = None
        utility = None
        for item in features[1:]:
            feature = item.css("::text").extract()[1]
            if ":" in feature:
                if "Arredato" in feature.split(": ")[1]:
                    furnished = True
                elif "Provincia" in feature:
                    city = feature.split(": ")[1].strip()
                elif "Deposito" in feature:
                    deposit = feature.split("€ ")[1].strip()
                    if "." in deposit:
                        deposit_array = deposit.split(".")
                        deposit = deposit_array[0] + deposit_array[1]

                    if ',' in deposit:
                        deposit = deposit.split(',')[0]
                    deposit = int(deposit)
                elif "Condominio mensile" in feature:
                    utility = feature.split(" ")[4].strip()

        coords = response.xpath(
            '/html/body/div[3]/div/div/div/div/div[4]/div[1]/div[5]/div/script[2]/text()').get()
        lat = None
        lng = None
        if coords:
            coords = coords.split("setLngLat([")[1].split("])")[0].strip()
            lat = coords.split(", ")[0]
            lng = coords.split(", ")[1]

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", int(square_meters))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", response.meta["address"])
        item_loader.add_value("city", city)
        item_loader.add_value("furnished", furnished)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("floor_plan_images", floor_plan)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", rent)
        item_loader.add_value("deposit", deposit)
        item_loader.add_value("utilities", utility)
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "390276017010")
        item_loader.add_value("landlord_email", "contact@houseloft.com")
        item_loader.add_value("landlord_name", "House & Loft")

        yield item_loader.load_item()
