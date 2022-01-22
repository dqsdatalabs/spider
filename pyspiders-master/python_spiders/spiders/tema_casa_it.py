# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class TemaCasaItSpider(scrapy.Spider):
    name = 'tema_casa_it'
    allowed_domains = ['tema-casa.it']
    start_urls = [
        'https://tema-casa.it/index.php?category_ids%5B%5D=13&property_types%5B%5D=3&min_price=0&max_price=1000000&keyword=&sortby=a.isFeatured&orderby=desc&address=&state_id=&postcode=&se_geoloc=&radius_search=5&nbath=&nbed=&nfloors=&nroom=&sqft_min=&sqft_max=&lotsize_min=&lotsize_max=&created_from=&created_to=&advfieldLists=&currency_item=&live_site=https%3A%2F%2Ftema-casa.it%2F&limitstart=0&process_element=&option=com_osproperty&task=property_advsearch&show_more_div=0&Itemid=127&search_param=catid%3A13_country%3A92_max_price%3A1000000_sortby%3Aa.isFeatured_orderby%3Adesc&list_id=0&adv_type=0&show_advancesearchform=1&advtype_id_2=88%2C86%2C96%2C97%2C100%2C101%2C90%2C91%2C102%2C103%2C89%2C87%2C92&advtype_id_1=88%2C86%2C96%2C97%2C100%2C101%2C90%2C91%2C102%2C89%2C87%2C92%2C106%2C108%2C109%2C107&advtype_id_3=88%2C86%2C96%2C97%2C100%2C101%2C90%2C91%2C102%2C103%2C89%2C87%2C92%2C106%2C108%2C109%2C107&advtype_id_4=88%2C86%2C96%2C97%2C100%2C101%2C90%2C91%2C102%2C89%2C87%2C92&advtype_id_5=88%2C86%2C96%2C97%2C100%2C101%2C90%2C91%2C102%2C89%2C87%2C92%2C106%2C108%2C109%2C107']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    # 1. FOLLOWING
    def parse(self, response):
        for appartment in response.css("div.span4.property_item"):
            cost = appartment.css(
                "span.listing-price::text").get().split(" ")[1]
            title = appartment.css("h5.marB0 > a::text").get()
            space = appartment.css(
                "ul.marB0>li:last-child>span.right.width50pc::text").get()

            url = "https://tema-casa.it/" + \
                appartment.css("div.span12>figure>a").attrib['href']

            yield Request(url,
                          callback=self.populate_item,
                          dont_filter=True,
                          meta={"title": title,
                                "cost": cost,
                                "space": space}
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.css(
            "#propertydetails > div > div.row-fluid.summary.singleTop > div > h1 > span.label.label-warning.categorylabel > a::text").get()

        if "monolocale" in property_type.lower() or "trilocale" in property_type.lower() or "bilocale" in property_type.lower():
            property_type = 'apartment'

        description = response.css("div.entry-content::text").get()

        description_text = response.css(
            "div.entry-content>p>span>span::text").extract()
        address = ""
        for i in range(len(description_text)):
            if i == len(description_text)-1:
                address = description_text[i]
            else:
                description += ","
                description += str(description_text[i])

        if len(address) < 2:
            return

        bathrooms = response.css(
            '#shellfeatures > div > div > div > div:nth-child(1) > div:nth-child(1) > div:nth-child(4) > div > div.fieldvalue::text').get()

        rooms = response.css(
            '#shellfeatures > div > div > div > div:nth-child(1) > div:nth-child(1) > div:nth-child(3) > div > div.fieldvalue::text').get()

        images = response.css(
            'img.pictureslideshow::attr(src)').extract()

        external_id = response.url.split('id=')[1].split('&')[0]

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", response.meta["title"])
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", int(
            response.meta["space"].strip()))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", "Rome")

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(response.meta["cost"].strip()))
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", "3938582471")
        item_loader.add_value("landlord_email", "info@tema-casa.it")
        item_loader.add_value("landlord_name", "Tema Casa")

        yield item_loader.load_item()
